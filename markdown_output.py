#!/usr/bin/env python3
"""
Markdown output generator for beer tracking data.
Captures the most recent output and saves it as a formatted markdown file.
"""

import duckdb
import json
import ast
from datetime import datetime
from typing import Dict, List, Any, Optional


class MarkdownOutputGenerator:
    """Generates markdown reports from beer tracking data."""

    def __init__(self, db_path: str = "beer_tracking.duckdb", output_file: str = "latest_output.md"):
        self.db_path = db_path
        self.output_file = output_file

    def generate_markdown_report(self) -> bool:
        """Generate a complete markdown report of the latest beer tracking data."""
        try:
            conn = duckdb.connect(self.db_path)

            # Check if tables exist
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [table[0] for table in tables]

            if not table_names:
                self._write_empty_report()
                return True

            markdown_content = self._build_markdown_content(conn, table_names)

            with open(self.output_file, 'w', encoding='utf-8') as f:
                f.write(markdown_content)

            conn.close()
            print(f"✅ Markdown report saved to {self.output_file}")
            return True

        except Exception as e:
            print(f"❌ Error generating markdown report: {e}")
            return False

    def _build_markdown_content(self, conn: duckdb.DuckDBPyConnection, table_names: List[str]) -> str:
        """Build the complete markdown content."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        content = [
            "# Beer Tracking Report",
            f"*Generated on {timestamp}*",
            "",
        ]

        # Add beer snapshots section
        if "beer_snapshots" in table_names:
            content.extend(self._build_snapshots_section(conn))

        # Add beer changes section
        if "beer_changes" in table_names:
            content.extend(self._build_changes_section(conn))

        # Add summary section
        content.extend(self._build_summary_section(conn, table_names))

        return "\n".join(content)

    def _build_snapshots_section(self, conn: duckdb.DuckDBPyConnection) -> List[str]:
        """Build the beer snapshots section."""
        content = ["## 📸 Beer Snapshots", ""]

        try:
            # Get recent snapshots
            snapshots = conn.execute("""
                SELECT timestamp, beer_count
                FROM beer_snapshots
                ORDER BY timestamp DESC
                LIMIT 10
            """).fetchall()

            if not snapshots:
                content.append("*No snapshots found.*")
                content.append("")
                return content

            content.extend([
                "| Timestamp | Beer Count |",
                "|-----------|------------|"
            ])

            for snapshot in snapshots:
                content.append(f"| {snapshot[0]} | {snapshot[1]} |")

            content.append("")

            # Add latest beer list details
            latest_snapshot = conn.execute("""
                SELECT snapshot_data
                FROM beer_snapshots
                ORDER BY timestamp DESC
                LIMIT 1
            """).fetchone()

            if latest_snapshot:
                content.extend(self._build_latest_beer_list(latest_snapshot[0]))

        except Exception as e:
            content.append(f"*Error loading snapshots: {e}*")

        content.append("")
        return content

    def _build_latest_beer_list(self, snapshot_data: Any) -> List[str]:
        """Build the latest beer list section."""
        content = ["### 🍺 Current Beer List", ""]

        try:
            if isinstance(snapshot_data, str):
                data = json.loads(snapshot_data)
            else:
                data = snapshot_data

            beers = data.get('beers', [])

            # Group by vessel type
            drafts = [beer for beer in beers if beer.get('vessel') == 'Draft']
            bottles_cans = [beer for beer in beers if beer.get('vessel') == 'Bottle/Can']

            if drafts:
                content.extend([
                    f"#### 🍺 Draft Beers ({len(drafts)})",
                    "",
                    "| Name | Style | ABV | Location |",
                    "|------|-------|-----|----------|"
                ])

                for beer in drafts[:20]:  # Show first 20
                    name = beer.get('name', 'Unknown')
                    style = beer.get('style', 'Unknown')
                    abv = beer.get('abv', 'Unknown')
                    location = beer.get('brewery_location', 'Unknown')
                    content.append(f"| {name} | {style} | {abv} | {location} |")

                if len(drafts) > 20:
                    content.append(f"| *... and {len(drafts) - 20} more* | | | |")

                content.append("")

            if bottles_cans:
                content.extend([
                    f"#### 🥫 Bottles & Cans ({len(bottles_cans)})",
                    "",
                    "| Name | Style | ABV | Location |",
                    "|------|-------|-----|----------|"
                ])

                for beer in bottles_cans[:20]:  # Show first 20
                    name = beer.get('name', 'Unknown')
                    style = beer.get('style', 'Unknown')
                    abv = beer.get('abv', 'Unknown')
                    location = beer.get('brewery_location', 'Unknown')
                    content.append(f"| {name} | {style} | {abv} | {location} |")

                if len(bottles_cans) > 20:
                    content.append(f"| *... and {len(bottles_cans) - 20} more* | | | |")

                content.append("")

        except Exception as e:
            content.append(f"*Error parsing beer list: {e}*")

        return content

    def _build_changes_section(self, conn: duckdb.DuckDBPyConnection) -> List[str]:
        """Build the beer changes section."""
        content = ["## 🔄 Recent Changes", ""]

        try:
            changes = conn.execute("""
                SELECT timestamp, added_count, removed_count, total_changes
                FROM beer_changes
                ORDER BY timestamp DESC
                LIMIT 10
            """).fetchall()

            if not changes:
                content.append("*No changes recorded.*")
                content.append("")
                return content

            content.extend([
                "| Timestamp | Added | Removed | Total Changes |",
                "|-----------|-------|---------|---------------|"
            ])

            for change in changes:
                content.append(f"| {change[0]} | {change[1]} | {change[2]} | {change[3]} |")

            content.append("")

            # Add latest changes details
            latest_change = conn.execute("""
                SELECT changes_data
                FROM beer_changes
                ORDER BY timestamp DESC
                LIMIT 1
            """).fetchone()

            if latest_change:
                content.extend(self._build_latest_changes_details(latest_change[0]))

        except Exception as e:
            content.append(f"*Error loading changes: {e}*")

        content.append("")
        return content

    def _build_latest_changes_details(self, changes_data: Any) -> List[str]:
        """Build the latest changes details section."""
        content = ["### 🆕 Latest Changes Details", ""]

        try:
            if isinstance(changes_data, str):
                data = json.loads(changes_data)
            else:
                data = changes_data

            added_beers = data.get('added_beers', [])
            removed_beers = data.get('removed_beers', [])

            if added_beers:
                content.extend([
                    "#### ➕ Added Beers",
                    "",
                    "| Name | Style | ABV | Vessel | Location |",
                    "|------|-------|-----|--------|----------|"
                ])
                for beer_data in added_beers[:10]:  # Show first 10
                    if isinstance(beer_data, dict):
                        name = beer_data.get('name', 'Unknown')
                        style = beer_data.get('style', 'Unknown')
                        abv = beer_data.get('abv', 'Unknown')
                        vessel = beer_data.get('vessel', 'Unknown')
                        location = beer_data.get('brewery_location', 'Unknown')
                        content.append(f"| {name} | {style} | {abv} | {vessel} | {location} |")
                    else:
                        # Fallback for legacy string data
                        try:
                            beer_dict = ast.literal_eval(str(beer_data))
                            name = beer_dict.get('name', 'Unknown')
                            style = beer_dict.get('style', 'Unknown')
                            abv = beer_dict.get('abv', 'Unknown')
                            vessel = beer_dict.get('vessel', 'Unknown')
                            location = beer_dict.get('brewery_location', 'Unknown')
                            content.append(f"| {name} | {style} | {abv} | {vessel} | {location} |")
                        except:
                            content.append(f"| {str(beer_data)[:50]}... | - | - | - | - |")

                if len(added_beers) > 10:
                    content.append(f"| *... and {len(added_beers) - 10} more* | | | | |")
                content.append("")

            if removed_beers:
                content.extend([
                    "#### ➖ Removed Beers",
                    "",
                    "| Name | Style | ABV | Vessel | Location |",
                    "|------|-------|-----|--------|----------|"
                ])
                for beer_data in removed_beers[:10]:  # Show first 10
                    if isinstance(beer_data, dict):
                        name = beer_data.get('name', 'Unknown')
                        style = beer_data.get('style', 'Unknown')
                        abv = beer_data.get('abv', 'Unknown')
                        vessel = beer_data.get('vessel', 'Unknown')
                        location = beer_data.get('brewery_location', 'Unknown')
                        content.append(f"| {name} | {style} | {abv} | {vessel} | {location} |")
                    else:
                        # Fallback for legacy string data
                        try:
                            beer_dict = ast.literal_eval(str(beer_data))
                            name = beer_dict.get('name', 'Unknown')
                            style = beer_dict.get('style', 'Unknown')
                            abv = beer_dict.get('abv', 'Unknown')
                            vessel = beer_dict.get('vessel', 'Unknown')
                            location = beer_dict.get('brewery_location', 'Unknown')
                            content.append(f"| {name} | {style} | {abv} | {vessel} | {location} |")
                        except:
                            content.append(f"| {str(beer_data)[:50]}... | - | - | - | - |")

                if len(removed_beers) > 10:
                    content.append(f"| *... and {len(removed_beers) - 10} more* | | | | |")
                content.append("")

            if not added_beers and not removed_beers:
                content.append("*No changes in the latest update.*")
                content.append("")

        except Exception as e:
            content.append(f"*Error parsing changes data: {e}*")
            content.append("")

        return content

    def _build_summary_section(self, conn: duckdb.DuckDBPyConnection, table_names: List[str]) -> List[str]:
        """Build the summary section."""
        content = ["## 📊 Summary", ""]

        try:
            summary_data = {}

            # Get total snapshots
            if "beer_snapshots" in table_names:
                total_snapshots = conn.execute("SELECT COUNT(*) FROM beer_snapshots").fetchone()[0]
                summary_data['Total Snapshots'] = total_snapshots

                # Get latest beer count
                latest_count = conn.execute("""
                    SELECT beer_count FROM beer_snapshots
                    ORDER BY timestamp DESC LIMIT 1
                """).fetchone()
                if latest_count:
                    summary_data['Current Beer Count'] = latest_count[0]

            # Get total changes
            if "beer_changes" in table_names:
                total_changes = conn.execute("SELECT COUNT(*) FROM beer_changes").fetchone()[0]
                summary_data['Total Change Records'] = total_changes

            # Get date range
            if "beer_snapshots" in table_names:
                date_range = conn.execute("""
                    SELECT MIN(timestamp), MAX(timestamp) FROM beer_snapshots
                """).fetchone()
                if date_range[0] and date_range[1]:
                    summary_data['Data Range'] = f"{date_range[0]} to {date_range[1]}"

            if summary_data:
                for key, value in summary_data.items():
                    content.append(f"- **{key}**: {value}")
            else:
                content.append("*No summary data available.*")

        except Exception as e:
            content.append(f"*Error generating summary: {e}*")

        content.append("")
        return content

    def _write_empty_report(self):
        """Write an empty report when no data is available."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        content = f"""# Beer Tracking Report
*Generated on {timestamp}*

## Status
No data tables found in the database. The beer tracking system may not have been initialized yet.

## Next Steps
1. Ensure the Google Sheet URL is configured
2. Run the beer tracking pipeline to collect initial data
3. Check that the database is properly connected
"""

        with open(self.output_file, 'w', encoding='utf-8') as f:
            f.write(content)


def generate_markdown_output(output_file: str = "latest_output.md") -> bool:
    """Convenience function to generate markdown output."""
    generator = MarkdownOutputGenerator(output_file=output_file)
    return generator.generate_markdown_report()


if __name__ == "__main__":
    success = generate_markdown_output()
    exit(0 if success else 1)