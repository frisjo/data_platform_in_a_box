import dagster as dg
import requests

def fetch_GBGS_data(context: dg.AssetExecutionContext):

    GBGS_api_client = context.resources.GBGS_api_client
    url = GBGS_api_client.base_url

    while True:
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            yield results

            next_url = data.get("next")

            if next_url:
                url = next_url
            else:
                break

        else:
            raise RuntimeError(f"API request failed with status code: {response.status_code}")
        
def fetch_TV_data(context: dg.AssetExecutionContext):
    
    TV_api_client = context.resources.TV_api_client
    response = TV_api_client.response
    print(response)
    
    if response.status_code == 200:
        data = response.json()

        if 'RESPONSE' in data and 'RESULT' in data['RESPONSE']:
            result = data['RESPONSE']['RESULT'][0]

        if "TrafficFlow" in result:
            traffic_data = result['TrafficFlow']

        # # Neglect old data by filtering on current minute (maybe not needed since DLT handles)
        # current_minute = arrow.now().floor('minute')
        # print(current_minute)
        # current_traffic_data = [row for row in traffic_data 
        #                 if arrow.get(row['MeasurementTime']).floor('minute') == current_minute]

        yield traffic_data

    else:
        raise RuntimeError(f"API request failed with status code: {response.status_code}")