import dagster as dg
import requests
import os
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