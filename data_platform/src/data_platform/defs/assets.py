import dagster as dg
import requests
from pathlib import Path
import dlt
from .fetch_data import fetch_GBGS_data, fetch_TV_data
from dagster_dbt import DbtProject
from dagster_dbt import DbtCliResource, dbt_assets


""" Asset for fetching and loading the air quality data into duckdb """
@dg.asset(
    kinds={"python", "dlt", "duckdb"},
    required_resource_keys={"GBGS_api_client"}
)
def GBGS_raw_data(context: dg.AssetExecutionContext):

    pipeline = dlt.pipeline(
        destination=dlt.destinations.duckdb("data/air_quality.duckdb"),
        dataset_name="air_quality_data"
    )

    info = pipeline.run(
        fetch_GBGS_data(context),
        table_name="GBGS_air_quality_data",
        write_disposition="merge",
        primary_key=["date", "time"]
    )

    context.log.info(f"Loaded {info.loads_ids}")
    return info

""" Asset for fetching and loading the traffic flow data into duckdb """
@dg.asset(
    kinds={"python", "dlt", "duckdb"},
    required_resource_keys={"TV_api_client"}
)
def TV_raw_data(context: dg.AssetExecutionContext):

    pipeline = dlt.pipeline(
        destination=dlt.destinations.duckdb("data/air_quality.duckdb"),
        dataset_name="traffic_flow_data"
    )

    info = pipeline.run(
        fetch_TV_data(context),
        table_name="TV_traffic_flow_data",
        write_disposition="merge",
        primary_key=["SiteId", "MeasurementTime"]
    )

    context.log.info(f"Loaded {info.loads_ids}")
    return info
    
""" dbt assets """

# Get paths to dbt_project.yml and profiles.yml
# Local path
# current_file = Path(__file__).resolve()
# transformations_dir = current_file.parent.parent.parent.parent / "transformations"

# Container path
transformations_dir = Path("/opt/dagster/app/transformations")

# Create dbt project (represent dbt project structure and metadata)
# Used for getting manifest path, to understand dbt models and relationships - what Dagster reads to understand what assets to create
air_quality_project = DbtProject(
    project_dir=str(transformations_dir),
    profiles_dir=str(transformations_dir),
)

air_quality_project.prepare_if_dev()

# Create dbt assets (all dbt models in dbt project)
@dbt_assets(
    manifest=air_quality_project.manifest_path # dbt's complied project representations in dbt/target/ - for dagster to 'understand' dbt models and their relationships
)
def air_quality_dbt_assets(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream() # manifest generated when running dbt commands (build/run...)