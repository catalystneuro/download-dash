import os

import requests

_KNOWN_SERVICES = ("GitHub", "AWS", "GCP", "VPN")  # Azure has problems; see _ip_utils.py for more info

_DEFAULT_REGION_CODES_TO_COORDINATES = {
    # Included for testing/demo purposes
    "AWS/us-east-2": {"latitude": 39.9612, "longitude": -82.9988},
    "GCP/us-central1": {"latitude": 41.2619, "longitude": -95.8608},
    # Explicit entries that fail heuristic matching
    "BO/La Paz Department": {"latitude": -11.7773231, "longitude": -67.4519752},
    "CL/Valparaíso": {"latitude": -32.5976089, "longitude": -70.8529753},
    "CN/Hainan": {"latitude": 19.2000001, "longitude": 109.5999999},
    "CO/Huila Department": {"latitude": 2.53593490, "longitude": -75.52766990},
    "CO/Risaralda Department": {"latitude": 5.2102948, "longitude": -75.9842236},
    "CO/Valle del Cauca Department": {"latitude": 3.788778, "longitude": -76.472768},
    "CR/San José": {"latitude": 9.9325427, "longitude": -84.0795782},
    "ES/Castille and León": {"latitude": 42.00, "longitude": -5.5},
    "FR/Grand Est": {"latitude": 48.580002, "longitude": 7.750000},
    "IQ/Sulaymaniyah": {"latitude": 35.5574725, "longitude": 45.435202},
    "JP/Ishikawa": {"latitude": 36.9890574, "longitude": 136.8162839},
    "MO/São Francisco Xavier": {"latitude": 22.210928, "longitude": 113.552971},
    "MX/México": {"latitude": 19.4326296, "longitude": -99.1331785},
    "NI/Managua Department": {"latitude": 12.125, "longitude": -86.31},
    "NO/Vestland": {"latitude": 60.9291011, "longitude": 6.1078869},
    "PA/Panamá": {"latitude": 8.559559, "longitude": -81.1308434},
    "PA/Panamá Oeste Province": {"latitude": 8.88028, "longitude": -79.78330},
    "PE/Lima Province": {"latitude": -12.5453873, "longitude": -75.8599243},
    "PL/Greater Poland": {"latitude": 52.406374, "longitude": 16.9251681},
    "PL/Pomerania": {"latitude": 53.428543, "longitude": 14.552811},
    "PL/Silesia": {"latitude": 50.6966393, "longitude": 17.9254068},
    "PL/Subcarpathia": {"latitude": 50.0575, "longitude": 22.0896},
    "PR/San Juan": {"latitude": 18.384239, "longitude": -66.05344},
    "RU/Rostov": {"latitude": 57.2012699, "longitude": 39.4221813},
    "TW/Takao": {"latitude": 22.6226696, "longitude": 120.2764261},
    "VN/Long An Povince": {"latitude": 10.56071680, "longitude": 106.64976230},
    "UY/Montevideo Department": {"latitude": -34.9058916, "longitude": -56.1913095},
    "RU/Mordoviya Republic": {"latitude": 54.5, "longitude": 44},  # OpenCage mistakes with Missouri
    "NZ/Taranaki Region": {"latitude": -39.3848064, "longitude": 174.1973505},  # There's a Taranaki in CL apparently
    "VN/Bình Phước Province": {"latitude": 11.749990, "longitude": 106.953958},
    "VN/Hà Nam Province": {"latitude": 20.583333, "longitude": 105.916667},
    "HN/Francisco Morazán Department": {"latitude": 14.0723, "longitude": -87.1921},
    "FR/Grand Est": {"latitude": 48.580002, "longitude": 7.750000},
    "SV/San Salvador Department": {"latitude": 13.6929, "longitude": -89.2182},
    "VN/Yên Bái Province": {"latitude": 21.666667, "longitude": 104.916667},
    
    # Skip unknowable entries
    "GitHub": {"latitude": None, "longitude": None},
    "VPN": {"latitude": None, "longitude": None},
    "unknown": {"latitude": None, "longitude": None},
}

opencage_api_key = os.environ.get("OPENCAGE_API_KEY", None)


def get_region_coordinates(region_code: str) -> dict:
    """
    Get the coordinates for a given region code.
    
    Args:
        region_code: The region code to look up.
        
    Returns:
        A dictionary with 'latitude' and 'longitude' keys, or None if not found.
    """

    if region_code in _KNOWN_SERVICES:
        return {"latitude": None, "longitude": None}
    
    # First check if we have an exact match in the predefined dictionary
    if region_code in _DEFAULT_REGION_CODES_TO_COORDINATES:
        return _DEFAULT_REGION_CODES_TO_COORDINATES[region_code]
    
    # Attempt to match using heuristic
    for key in _DEFAULT_REGION_CODES_TO_COORDINATES:
        if region_code.lower() in key.lower():
            return _DEFAULT_REGION_CODES_TO_COORDINATES[key]
    
    # use OpenCage API service to get coordinates
    if opencage_api_key:
        response = requests.get(
            f"https://api.opencagedata.com/geocode/v1/json?q={region_code}&key={opencage_api_key}"
        )
        if response.status_code == 200:
            data = response.json()
            if data["results"]:
                return {
                    "latitude": data["results"][0]["geometry"]["lat"],
                    "longitude": data["results"][0]["geometry"]["lng"],
                }
    return {"latitude": None, "longitude": None}
