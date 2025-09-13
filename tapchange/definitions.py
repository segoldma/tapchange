from dagster import Definitions
from .assets import current_beer_list, beer_list_snapshot, beer_list_changes
from .jobs import beer_tracking_job
from .schedules import daily_beer_check

defs = Definitions(
    assets=[
        current_beer_list,
        beer_list_snapshot,
        beer_list_changes
    ],
    jobs=[beer_tracking_job],
    schedules=[daily_beer_check],
)