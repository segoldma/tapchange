#!/usr/bin/env python3
"""
Standalone runner for beer tracking - designed for GitHub Actions.
Runs the Dagster assets without the web server.
"""

import os
from tapchange.assets import current_beer_list, beer_list_snapshot, beer_list_changes, BeerConfig

def run_beer_tracking():
    """Run all beer tracking assets in sequence."""

    # Set up config
    config = BeerConfig(
        google_sheet_url=os.getenv("GOOGLE_SHEET_URL", "")
    )

    print("🍺 Starting beer list tracking...")

    try:
        # Create a simple context object for logging
        class SimpleContext:
            def log_info(self, message):
                print(f"INFO: {message}")

            @property
            def log(self):
                return self

            def info(self, message):
                print(f"INFO: {message}")

        context = SimpleContext()

        # Step 1: Get current beer list
        print("📋 Fetching current beer list...")
        current_list = current_beer_list(context, config)
        print(f"✅ Retrieved {len(current_list)} beers")

        # Step 2: Create snapshot
        print("📸 Creating snapshot...")
        snapshot = beer_list_snapshot(context, current_list)
        print(f"✅ Snapshot created with {snapshot['beer_count']} beers")

        # Step 3: Detect changes
        print("🔍 Detecting changes...")
        changes = beer_list_changes(context, snapshot)
        print(f"✅ Found {changes['total_changes']} total changes:")
        print(f"   - Added: {len(changes['added_beers'])}")
        print(f"   - Removed: {len(changes['removed_beers'])}")

        # Print some details about changes
        if changes['added_beers']:
            print("🆕 New beers:")
            for beer in changes['added_beers'][:5]:  # Show first 5
                print(f"   + {beer}")
            if len(changes['added_beers']) > 5:
                print(f"   ... and {len(changes['added_beers']) - 5} more")

        if changes['removed_beers']:
            print("❌ Removed beers:")
            for beer in changes['removed_beers'][:5]:  # Show first 5
                print(f"   - {beer}")
            if len(changes['removed_beers']) > 5:
                print(f"   ... and {len(changes['removed_beers']) - 5} more")

        print("🎉 Beer tracking completed successfully!")
        return True

    except Exception as e:
        print(f"❌ Error during beer tracking: {e}")
        raise

if __name__ == "__main__":
    run_beer_tracking()