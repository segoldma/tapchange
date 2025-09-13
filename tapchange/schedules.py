from dagster import ScheduleDefinition, DefaultScheduleStatus
from .jobs import beer_tracking_job


# Schedule to run daily at noon
daily_beer_check = ScheduleDefinition(
    job=beer_tracking_job,
    cron_schedule="0 12 * * *",  # Daily at 12:00 PM (noon)
    default_status=DefaultScheduleStatus.RUNNING,
)