import requests

def send_soilgrids_request(lon, lat):
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
