import os
from datetime import datetime
from typing import List, Dict, Any
import duckdb
from dagster import asset, AssetExecutionContext, Config
from dotenv import load_dotenv

load_dotenv()


class BeerConfig(Config):
    google_sheet_url: str = os.getenv("GOOGLE_SHEET_URL", "")


@asset
def current_beer_list(context: AssetExecutionContext, config: BeerConfig) -> List[Dict[str, Any]]:
    """Raw beer list data from Google Sheet via DuckDB CSV export."""
    if not config.google_sheet_url:
        raise ValueError("GOOGLE_SHEET_URL environment variable is required")

    # Convert Google Sheets URL to CSV export URL
    # Extract the sheet ID from the URL
    sheet_id = config.google_sheet_url.split('/d/')[1].split('/')[0]
    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid=0"

    context.log.info(f"Fetching beer list from CSV export: {csv_url}")

    # Initialize DuckDB connection
    conn = duckdb.connect()

    # Install and load the https extension for secure HTTP requests
    conn.execute("INSTALL https")
    conn.execute("LOAD https")

    # Read raw CSV data with auto detection
    query = f"""
    SELECT * FROM read_csv_auto('{csv_url}',
        header=false,
        delim=',',
        quote='"',
        null_padding=true,
        ignore_errors=true,
        all_varchar=true
    )
    """
    raw_data = conn.execute(query).fetchall()

    # Parse the multi-section format
    beer_data = []
    current_section = None

    for row in raw_data:
        row_data = [cell for cell in row if cell and str(cell).strip()]

        if not row_data:  # Skip empty rows
            continue

        first_cell = str(row_data[0]).strip()

        # Detect section headers
        if first_cell == "Drafts":
            current_section = "Drafts"
            context.log.info("Found Drafts section")
            continue
        elif first_cell == "Bottles and Cans":
            current_section = "Bottles and Cans"
            context.log.info("Found Bottles and Cans section")
            continue

        # Skip if we haven't found a section yet or if this looks like a header row
        if not current_section or any(header in first_cell.lower() for header in ['name', 'beer', 'type', 'brewery']):
            continue

        # Parse beer entries based on section
        if current_section and len(row_data) >= 3:
            beer_entry = {
                "vessel": "Draft" if current_section == "Drafts" else "Bottle/Can",
                "name": row_data[0],
                "style": row_data[1] if len(row_data) > 1 else "",
                "brewery_location": row_data[2] if len(row_data) > 2 else "",
                "abv": row_data[3] if len(row_data) > 3 else "",
                "description": row_data[4] if len(row_data) > 4 else ""
            }
            beer_data.append(beer_entry)

    context.log.info(f"Retrieved {len(beer_data)} beer records ({len([b for b in beer_data if b['vessel'] == 'Draft'])} drafts, {len([b for b in beer_data if b['vessel'] == 'Bottle/Can'])} bottles/cans)")
    return beer_data


@asset(deps=[current_beer_list])
def beer_list_snapshot(context: AssetExecutionContext, current_beer_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Processed and timestamped snapshot of the current beer list."""

    # Add timestamp and normalize data
    snapshot = {
        "timestamp": datetime.now().isoformat(),
        "beer_count": len(current_beer_list),
        "beers": current_beer_list
    }

    # Store in DuckDB for persistence
    conn = duckdb.connect("beer_tracking.duckdb")

    # Create table if it doesn't exist
    conn.execute("""
        CREATE TABLE IF NOT EXISTS beer_snapshots (
            timestamp TIMESTAMP,
            beer_count INTEGER,
            snapshot_data JSON
        )
    """)

    # Insert current snapshot
    conn.execute(
        "INSERT INTO beer_snapshots VALUES (?, ?, ?)",
        [snapshot["timestamp"], snapshot["beer_count"], snapshot]
    )

    context.log.info(f"Stored snapshot with {snapshot['beer_count']} beers at {snapshot['timestamp']}")
    return snapshot


@asset(deps=[beer_list_snapshot])
def beer_list_changes(context: AssetExecutionContext, beer_list_snapshot: Dict[str, Any]) -> Dict[str, Any]:
    """Detected changes (additions/removals) between current and previous snapshots."""

    conn = duckdb.connect("beer_tracking.duckdb")

    # Get previous snapshot
    previous_snapshot = conn.execute("""
        SELECT snapshot_data
        FROM beer_snapshots
        WHERE timestamp < ?
        ORDER BY timestamp DESC
        LIMIT 1
    """, [beer_list_snapshot["timestamp"]]).fetchone()

    changes = {
        "timestamp": beer_list_snapshot["timestamp"],
        "added_beers": [],
        "removed_beers": [],
        "total_changes": 0
    }

    if previous_snapshot:
        previous_data = previous_snapshot[0]
        # Parse JSON if it's a string
        if isinstance(previous_data, str):
            import json
            previous_data = json.loads(previous_data)

        import json

        # Create comparable representations using JSON strings for set operations
        current_beer_strs = set(json.dumps(beer, sort_keys=True) for beer in beer_list_snapshot["beers"])
        previous_beer_strs = set(json.dumps(beer, sort_keys=True) for beer in previous_data["beers"])

        # Find additions and removals using JSON string comparison
        added_strs = current_beer_strs - previous_beer_strs
        removed_strs = previous_beer_strs - current_beer_strs

        # Convert back to actual beer dictionaries
        current_beers_lookup = {json.dumps(beer, sort_keys=True): beer for beer in beer_list_snapshot["beers"]}
        previous_beers_lookup = {json.dumps(beer, sort_keys=True): beer for beer in previous_data["beers"]}

        changes["added_beers"] = [current_beers_lookup[beer_str] for beer_str in added_strs]
        changes["removed_beers"] = [previous_beers_lookup[beer_str] for beer_str in removed_strs]
        changes["total_changes"] = len(changes["added_beers"]) + len(changes["removed_beers"])

        context.log.info(f"Found {len(changes['added_beers'])} new beers, {len(changes['removed_beers'])} removed beers")
    else:
        context.log.info("No previous snapshot found - this is the initial run")
        changes["total_changes"] = len(beer_list_snapshot["beers"])

    # Store changes
    conn.execute("""
        CREATE TABLE IF NOT EXISTS beer_changes (
            timestamp TIMESTAMP,
            added_count INTEGER,
            removed_count INTEGER,
            total_changes INTEGER,
            changes_data JSON
        )
    """)

    conn.execute(
        "INSERT INTO beer_changes VALUES (?, ?, ?, ?, ?)",
        [
            changes["timestamp"],
            len(changes["added_beers"]),
            len(changes["removed_beers"]),
            changes["total_changes"],
            changes
        ]
    )

    return changes