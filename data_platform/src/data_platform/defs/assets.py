import dagster as dg
from pathlib import Path
import dlt
import folium
from geopy.distance import geodesic
import json
import os

from .fetch_data import fetch_GBGS_data, fetch_TV_data

from dagster_dbt import DbtProject
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_duckdb import DuckDBResource

# Azure
from azure.storage.blob import BlobServiceClient

""" Asset for fetching and loading the air quality data into duckdb """
@dg.asset(
    kinds={"python", "dlt", "duckdb"},
    required_resource_keys={"GBGS_api_client"},
    io_manager_key="azure_duckdb_io_manager", 
    group_name="raw_data"
)
def GBGS_raw_data(context):

    az_duckdb_io_manager = context.resources.azure_duckdb_io_manager
    # Use load_input function of io manager to get connection to Azure blob storage container and temp local path
    # (conn, tmp_path): tuple från IO managern
    conn, tmp_path = az_duckdb_io_manager.load_input(context)

    context.log.info(f"Connected to local duckdb: {tmp_path}")

    pipeline = dlt.pipeline(
        destination=dlt.destinations.duckdb(conn),
        dataset_name="air_quality_data"
    )

    info = pipeline.run(
        fetch_GBGS_data(context),
        table_name="GBGS_air_quality_data",
        write_disposition="merge",
        primary_key=["date", "time"]
    )

    context.log.info(f"Loaded {info.loads_ids}")

    # Return tuple for IO Manager (to use in handle_output)
    return conn, tmp_path

""" Asset for fetching and loading the traffic flow data into duckdb """
@dg.asset(
    kinds={"python", "dlt", "duckdb"},
    required_resource_keys={"TV_api_client"},
    io_manager_key="azure_duckdb_io_manager",
    group_name="raw_data",
)
def TV_raw_data(context):

    az_duckdb_io_manager = context.resources.azure_duckdb_io_manager
    # Use load_input function of io manager to get connection to Azure blob storage container and temp local path
    # (conn, tmp_path): tuple från IO managern
    conn, tmp_path = az_duckdb_io_manager.load_input(context)

    context.log.info(f"Connected to local duckdb: {tmp_path}")

    pipeline = dlt.pipeline(
        destination=dlt.destinations.duckdb(conn),
        dataset_name="traffic_flow_data"
    )


    info = pipeline.run(
        fetch_TV_data(context),
        table_name="TV_traffic_flow_data",
        write_disposition="merge",
        primary_key=["SiteId", "MeasurementTime"]
    )

    context.log.info(f"Loaded {info.loads_ids}")

    # Return tuple for IO Manager (to use in handle_output)
    return conn, tmp_path


""" Asset for getting air quality stations coordinates and map """
@dg.asset(
    kinds={"python"},
    required_resource_keys={"monitoring_stations_data"},
    deps=[GBGS_raw_data],
    group_name = "maps"
)
def monitoring_station_locations_map(context: dg.AssetExecutionContext):

    monitoring_stations_data = context.resources.monitoring_stations_data
    coordinates = []
    station_names = []

    for loc in monitoring_stations_data["locations"]:
        coordinates.append(loc["coordinates"])
        station_names.append(loc["name"])

    gbg_center = [57.7089, 11.9746]
    map_gbg = folium.Map(location=gbg_center, zoom_start=13)

    for coord, station_name in zip(coordinates, station_names):
        folium.Marker(
            location=coord,
            popup=f"Lat: {coord[0]}, Lon: {coord[1]}, Station name: {station_name}",
            icon=folium.Icon(color='red', icon='info-sign')
        ).add_to(map_gbg)

    # Temporary local path
    local_map_path = "/tmp/monitoring_stations.html"
    map_gbg.save(local_map_path)

    # Upload to Azure blob storage
    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING env var")

    blob_service = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service.get_blob_client(container="dagster-storage", blob="maps/monitoring_stations.html")

    with open(local_map_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
        context.log.info("Uploaded monitoring_stations.html to Azure blob storage in maps folder")

    # Remove temporary local file
    os.remove(local_map_path)

    return coordinates, station_names


""" Asset for getting trafficflow detector coordinates and map """
@dg.asset(
    kinds={"python"},
    deps=[TV_raw_data],
    group_name = "maps"
)
def detector_locations_map(context: dg.AssetExecutionContext, database: DuckDBResource):

    query = """
    SELECT DISTINCT site_id, geometry__wgs84
    FROM traffic_flow_data.tv_traffic_flow_data
    """
    with database.get_connection() as conn:
        df = conn.execute(query).pl()

    coordinates = []
    site_ids = []
    
    for row in df.iter_rows(named=True):

        wgs84_str = row["geometry__wgs84"]
        stripped = wgs84_str.replace('POINT (', '').replace(')', '')
        lon_str, lat_str = stripped.strip().split()
        lat = float(lat_str)
        lon = float(lon_str)
        siteid = int(row['site_id'])
        coordinate = [lat, lon]
        coordinates.append(coordinate)
        site_ids.append(siteid)

    gbg_center = [57.7089, 11.9746]
    map_gbg = folium.Map(location=gbg_center, zoom_start=13)

    for coord, siteid in zip(coordinates, site_ids):
        folium.Marker(
            location=coord,
            popup=f"Lat: {coord[0]}, Lon: {coord[1]}, Siteid:{siteid}",
            icon=folium.Icon(color='blue', icon='info-sign')
        ).add_to(map_gbg)

    # Temporary local path
    local_map_path = "/tmp/detectors.html"
    map_gbg.save(local_map_path)

    # Upload to Azure blob storage
    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING env var")

    blob_service = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service.get_blob_client(container="dagster-storage", blob="maps/detectors.html")

    with open(local_map_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
        context.log.info("Uploaded detectors.html to Azure blob storage in maps folder")

    # Remove temporary local file
    os.remove(local_map_path)
    return coordinates, site_ids


""" Asset for matching monitoring stations to closest detector """
@dg.asset(
    kinds={"python"},
    group_name = "maps"
)
def mapping_station_to_detector(
    context: dg.AssetExecutionContext,
    monitoring_station_locations_map: tuple, 
    detector_locations_map: tuple
):
    coordinates_ms, station_names = monitoring_station_locations_map
    coordinates_d, site_ids = detector_locations_map

    matches = []
    
    for i, ms_coord in enumerate(coordinates_ms):
        min_distance = float('inf')
        closest_detector = None
        closest_id = None
        
        for j, det_coord in enumerate(coordinates_d):
            distance = geodesic(ms_coord, det_coord).kilometers
            
            if distance < min_distance:
                min_distance = distance
                closest_detector = det_coord
                closest_id = site_ids[j]
        
        matches.append({
            'monitoring_station_index': i,
            'monitoring_station_name': station_names[i] if i < len(station_names) else f"Station_{i}",
            'monitoring_coord': ms_coord,
            'closest_detector_id': closest_id,
            'closest_detector_coord': closest_detector,
            'distance_km': min_distance
        })
    
    # Temporary JSON file path
    local_json_path = "/tmp/station_detector_matches.json"
    with open(local_json_path, 'w') as f:
        json.dump(matches, f, indent=2, default=str)
    
    # Upload to Azure blob storage
    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING env var")

    blob_service = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service.get_blob_client(container="dagster-storage", blob="json_files/station_detector_matches.json")

    with open(local_json_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
        context.log.info("Uploaded station_detector_matches.json to Azure blob storage in json_files folder")

    # Remove temporary file
    os.remove(local_json_path)
    return matches


""" Asset for merging monitoring stations and detectors locations into map """
@dg.asset(
    kinds={"python"},
    group_name = "maps"
)
def merged_map(
    context: dg.AssetExecutionContext,
    monitoring_station_locations_map: tuple, 
    detector_locations_map: tuple,
    mapping_station_to_detector: list  
):
    coordinates_ms, station_names = monitoring_station_locations_map
    coordinates_d, site_ids = detector_locations_map
    matches = mapping_station_to_detector

    gbg_center = [57.7089, 11.9746]
    combined_map = folium.Map(location=gbg_center, zoom_start=13)
    
    # Add monitoring station markers
    for coord, name in zip(coordinates_ms, station_names):
        folium.Marker(
            location=coord,
            popup=f"Monitoring Station: {name}<br>Lat: {coord[0]}<br>Lon: {coord[1]}",
            icon=folium.Icon(color='red', icon='info-sign')
        ).add_to(combined_map)
    
    # Add detector markers
    for coord, siteid in zip(coordinates_d, site_ids):
        folium.Marker(
            location=coord,
            popup=f"Detector Site ID: {siteid}<br>Lat: {coord[0]}<br>Lon: {coord[1]}",
            icon=folium.Icon(color='blue', icon='info-sign')
        ).add_to(combined_map)
    
    # Add connection lines between matched stations and detectors
    for match in matches:
        ms_coord = match['monitoring_coord']
        det_coord = match['closest_detector_coord']
        station_name = match['monitoring_station_name']
        detector_id = match['closest_detector_id']
        distance = match['distance_km']
        
        # Add connection line
        folium.PolyLine(
            locations=[ms_coord, det_coord],
            color='green',
            weight=4,
            opacity=0.8,
            popup=f"Connection: {station_name} → {detector_id}<br>Distance: {distance:.2f} km"
        ).add_to(combined_map)
    
    # combined_map.save("/opt/dagster/app/data/maps/merged_map.html")

    # Temporary local path
    local_map_path = "/tmp/detectors.html"
    combined_map.save(local_map_path)

    # Upload to Azure blob storage
    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str:
        raise ValueError("Missing AZURE_STORAGE_CONNECTION_STRING env var")

    blob_service = BlobServiceClient.from_connection_string(conn_str)
    blob_client = blob_service.get_blob_client(container="dagster-storage", blob="maps/merged_map.html")

    with open(local_map_path, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
        context.log.info("Uploaded merged_map.html to Azure blob storage in maps folder")

    # Remove temporary local file
    os.remove(local_map_path)    
    return combined_map


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