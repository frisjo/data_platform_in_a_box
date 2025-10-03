from pathlib import Path
import os
from dagster import Definitions
from dagster_dbt import DbtCliResource

# from data_platform.defs.raw_data_assets import GBGS_raw_data, TV_raw_data
# from data_platform.defs.maps_assets import monitoring_station_locations_map, detector_locations_map, mapping_station_to_detector, merged_map
# from data_platform.defs.dbt_assets import air_quality_dbt_assets

from data_platform.defs.assets import GBGS_raw_data, TV_raw_data, monitoring_station_locations_map, detector_locations_map, merged_map, mapping_station_to_detector, air_quality_dbt_assets
from data_platform.defs.resources import GBGS_api_client, TV_api_client, monitoring_stations_data, database_resource
from data_platform.defs.jobs import GBGS_update_job, TV_update_job
from data_platform.defs.schedules import GBGS_update_schedule, TV_update_schedule
from data_platform.defs.io_managers.azure_duckdb_io_manager import azure_duckdb_io_manager

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
        monitoring_station_locations_map,
        detector_locations_map,
        merged_map,
        mapping_station_to_detector,
        air_quality_dbt_assets
    ],
    resources={
        "azure_duckdb_io_manager": azure_duckdb_io_manager.configured({
            "account_name": os.environ.get("AZURE_STORAGE_ACCOUNT_NAME"),
            "account_key": os.environ.get("AZURE_STORAGE_ACCOUNT_KEY"),
            "container": os.environ.get("AZURE_STORAGE_ACCOUNT_CONTAINER"),
            "database_path": os.environ.get("AZURE_STORAGE_ACCOUNT_DATABASE_PATH"),
        }),
        "GBGS_api_client": GBGS_api_client,
        "TV_api_client": TV_api_client,
        "monitoring_stations_data": monitoring_stations_data,
        "database": database_resource,
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