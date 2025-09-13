#!/usr/bin/env python3
"""
Standalone runner for beer tracking - designed for GitHub Actions.
Runs the Dagster assets using proper materialization.
"""

import os
from dagster import materialize
from tapchange.assets import current_beer_list, beer_list_snapshot, beer_list_changes

def run_beer_tracking():
    """Run all beer tracking assets in sequence using Dagster materialization."""

    print("🍺 Starting beer list tracking...")

    try:
        # Set environment variable for assets to use
        if not os.getenv("GOOGLE_SHEET_URL"):
            print("❌ GOOGLE_SHEET_URL environment variable is required")
            return False

        # Materialize all assets in order
        print("📋 Running beer tracking pipeline...")

        result = materialize(
            assets=[current_beer_list, beer_list_snapshot, beer_list_changes],
            run_config={
                "ops": {
                    "current_beer_list": {
                        "config": {
                            "google_sheet_url": os.getenv("GOOGLE_SHEET_URL")
                        }
                    }
                }
            }
        )

        if result.success:
            print("✅ Beer tracking completed successfully!")

            # Get the final results
            changes_result = result.asset_materializations_for_node("beer_list_changes")
            if changes_result:
                print("🔍 Changes detected and logged to database")

            return True
        else:
            print("❌ Pipeline execution failed")
            return False

    except Exception as e:
        print(f"❌ Error during beer tracking: {e}")
        return False

if __name__ == "__main__":
    success = run_beer_tracking()
    exit(0 if success else 1)