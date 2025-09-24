from pathlib import Path
from dagster import Definitions
from dagster_dbt import DbtCliResource

from data_platform.defs.assets import GBGS_raw_data, TV_raw_data, air_quality_dbt_assets
from data_platform.defs.resources import GBGS_api_client, TV_api_client
from data_platform.defs.jobs import GBGS_update_job, TV_update_job
from data_platform.defs.schedules import GBGS_update_schedule, TV_update_schedule

# dbt resource
# # Local path
# current_file = Path(__file__).resolve()
# transformations_dir = current_file.parent.parent.parent / "transformations"

# Container Path
transformations_dir = Path("/opt/dagster/app/transformations")

# Define dbt resource
# Execution engine for running dbt commands (dbt build/run/test)
dbt_resource = DbtCliResource(
    project_dir=str(transformations_dir),
    profiles_dir=str(transformations_dir),
)

defs = Definitions(
    assets=[
        GBGS_raw_data,
        TV_raw_data,
        air_quality_dbt_assets
    ],
    resources={
        "GBGS_api_client": GBGS_api_client,
        "TV_api_client": TV_api_client,
        "dbt": dbt_resource
    },
    jobs=[
        GBGS_update_job,
        TV_update_job
    ],
    schedules=[
        GBGS_update_schedule,
        TV_update_schedule
    ]
)