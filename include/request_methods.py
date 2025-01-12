import requests
from datetime import datetime, timedelta


def soilgrids_request(lon, lat):
    url = 'https://rest.isric.org/soilgrids/v2.0/properties/query'
    parameters = {
        'lon': lon,
        'lat': lat,
        'property': [
            'bdod', 'cec', 'clay', 'nitrogen', 'phh2o', 'sand', 'silt', 'soc'
        ],
        'depth': ['0-5cm', '0-15cm', '15-30cm'],
        'value': ['mean', 'uncertainty']
    }
    response = requests.get(url, params=parameters)
    response.raise_for_status()
    return response.json()


def meteo_data_request(lon, lat, key, days=1):
    try:
        url = 'https://api.stormglass.io/v2/weather/point'
        end = datetime.now().date()
        start = end - timedelta(days=days)
        
        headers = {
            'Authorization': key
        }
        parameters = {
            'lat': lat,
            'lng': lon,
            'params': 'airTemperature,pressure,humidity,precipitation',
            'start': start.isoformat(),
            'end': end.isoformat(),
            'source': 'sg'
        }
        
        response = requests.get(url, params=parameters, headers=headers)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        raise
    