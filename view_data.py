#!/usr/bin/env python3
"""Simple script to view beer tracking data from DuckDB."""

import duckdb
import json
from datetime import datetime

def view_data():
    """View all data stored in the beer tracking database."""
    conn = duckdb.connect("beer_tracking.duckdb")W

    print("🍺 BEER TRACKING DATA 🍺")
    print("=" * 50)

    # Check if tables exist
    tables = conn.execute("SHOW TABLES").fetchall()
    print(f"Available tables: {[table[0] for table in tables]}")
    print()

    # View beer snapshots
    if any("beer_snapshots" in table for table in tables):
        print("📸 BEER SNAPSHOTS:")
        print("-" * 30)
        snapshots = conn.execute("""
            SELECT timestamp, beer_count
            FROM beer_snapshots
            ORDER BY timestamp DESC
        """).fetchall()

        for snapshot in snapshots:
            print(f"  {snapshot[0]} - {snapshot[1]} beers")
        print()

        # Show latest snapshot details
        if snapshots:
            print("🍺 LATEST BEER LIST:")
            print("-" * 30)
            latest = conn.execute("""
                SELECT snapshot_data
                FROM beer_snapshots
                ORDER BY timestamp DESC
                LIMIT 1
            """).fetchone()

            if latest:
                data = latest[0]
                if isinstance(data, str):
                    data = json.loads(data)

                # Group beers by vessel type
                drafts = [beer for beer in data['beers'] if beer.get('vessel') == 'Draft']
                bottles_cans = [beer for beer in data['beers'] if beer.get('vessel') == 'Bottle/Can']

                print(f"🍺 DRAFTS ({len(drafts)}):")
                for i, beer in enumerate(drafts[:10], 1):  # Show first 10 drafts
                    print(f"  {i}. {beer['name']} - {beer['style']} ({beer['abv']}) - {beer['brewery_location']}")

                if len(drafts) > 10:
                    print(f"    ... and {len(drafts) - 10} more drafts")

                print(f"\n🥫 BOTTLES & CANS ({len(bottles_cans)}):")
                for i, beer in enumerate(bottles_cans[:10], 1):  # Show first 10 bottles/cans
                    print(f"  {i}. {beer['name']} - {beer['style']} ({beer['abv']}) - {beer['brewery_location']}")

                if len(bottles_cans) > 10:
                    print(f"    ... and {len(bottles_cans) - 10} more bottles/cans")
                print()

    # View beer changes
    if any("beer_changes" in table for table in tables):
        print("🔄 BEER CHANGES:")
        print("-" * 30)
        changes = conn.execute("""
            SELECT timestamp, added_count, removed_count, total_changes
            FROM beer_changes
            ORDER BY timestamp DESC
        """).fetchall()

        for change in changes:
            print(f"  {change[0]} - Added: {change[1]}, Removed: {change[2]}, Total: {change[3]}")

        # Show latest changes details
        if changes:
            print()
            print("🆕 LATEST CHANGES:")
            print("-" * 30)
            latest_change = conn.execute("""
                SELECT changes_data
                FROM beer_changes
                ORDER BY timestamp DESC
                LIMIT 1
            """).fetchone()

            if latest_change:
                change_data = latest_change[0]
                if isinstance(change_data, str):
                    change_data = json.loads(change_data)

                if change_data['added_beers']:
                    print("  Added:")
                    for beer in change_data['added_beers'][:5]:  # Show first 5
                        print(f"    + {beer}")

                if change_data['removed_beers']:
                    print("  Removed:")
                    for beer in change_data['removed_beers'][:5]:  # Show first 5
                        print(f"    - {beer}")

    conn.close()

if __name__ == "__main__":
    view_data()