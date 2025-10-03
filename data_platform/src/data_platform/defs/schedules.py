import dagster as dg
from data_platform.defs.jobs import GBGS_update_job, TV_update_job

# Create schedule for GBGS update job
GBGS_update_schedule = dg.ScheduleDefinition(
    job=GBGS_update_job,
    cron_schedule="0 * * * *" # every hour
)

# Create schedule for GBGS update job
TV_update_schedule = dg.ScheduleDefinition(
    job=TV_update_job,
    cron_schedule="*/15 * * * *" # every 15th minute starting on top of every hour 
)