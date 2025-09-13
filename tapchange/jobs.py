from dagster import define_asset_job, AssetSelection
from .assets import current_beer_list, beer_list_snapshot, beer_list_changes

# Job that runs all beer tracking assets
beer_tracking_job = define_asset_job(
    name="beer_tracking_job",
    selection=AssetSelection.assets(
        current_beer_list,
        beer_list_snapshot,
        beer_list_changes
    ),
)