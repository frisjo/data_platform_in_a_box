import dagster as dg
from dagster_duckdb import DuckDBResource
import requests
import os
from pathlib import Path
import json

from adlfs import AzureBlobFileSystem
from dagster import ConfigurableResource
import tempfile
import duckdb

from dotenv import load_dotenv
load_dotenv()

@dg.resource()
def GBGS_api_client():
    GBGS_API_URL = os.getenv("GOTEBORGS_STAD_API_URL")

    class GBGSAPIClient:
        def __init__(self):
            self.base_url = GBGS_API_URL
        
    return GBGSAPIClient()

@dg.resource()
def TV_api_client():

    TV_API_URL = os.getenv("TRAFIKVERKET_API_URL")
    TV_API_KEY = os.getenv("TRAFIKVERKET_API_KEY")

    # CountyNo = '14' (Västra Götalands län)
    xml_request = f"""
    <REQUEST>
        <LOGIN authenticationkey="{TV_API_KEY}" />
        <QUERY objecttype="TrafficFlow" schemaversion="1">
            <FILTER>
                <EQ name="CountyNo" value="14" />
            </FILTER>
        </QUERY>
    </REQUEST>
    """

    class TVAPIClient:
        def __init__(self):
            self.response = requests.post(TV_API_URL, data=xml_request, headers={"Content-Type": "text/xml; charset=utf-8"})

    return TVAPIClient()

@dg.resource()
def monitoring_stations_data():

    monitoring_stations_data_path = Path("/opt/dagster/app/data/monitoring_stations_data/monitoring_stations.json")
    with open(monitoring_stations_data_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
        
    return data

# Create resource for duckdb
database_resource = DuckDBResource(
    database="/opt/dagster/app/data/air_quality.duckdb"
)

# class AzureDuckDBResource(ConfigurableResource):
#     """Resource for managing DuckDB databases in Azure Blob Storage"""
#     account_name: str
#     account_key: str
#     container: str
#     database_path: str  
    
#     def get_connection(self):
#         """Download database from Azure and return a connection"""
#         fs = AzureBlobFileSystem(
#             account_name=self.account_name,
#             account_key=self.account_key
#         )
        
#         # Create temp file for local database
#         tmp_path = tempfile.mktemp(suffix='.duckdb')
#         remote_path = f"{self.container}/{self.database_path}"
        
#         # Download from Azure if exists
#         if fs.exists(remote_path):
#             with fs.open(remote_path, 'rb') as remote_file:
#                 with open(tmp_path, 'wb') as local_file:
#                     local_file.write(remote_file.read())
        
#         return duckdb.connect(tmp_path), tmp_path, fs, remote_path
    
#     def upload_database(self, local_path: str):
#         """Upload database back to Azure"""
#         fs = AzureBlobFileSystem(
#             account_name=self.account_name,
#             account_key=self.account_key
#         )
        
#         remote_path = f"{self.container}/{self.database_path}"
        
#         with open(local_path, 'rb') as local_file:
#             with fs.open(remote_path, 'wb') as remote_file:
#                 remote_file.write(local_file.read())