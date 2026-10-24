
import os
import requests
from dotenv import load_dotenv

# Load API key from the .env file in the project root
load_dotenv()

API_KEY = os.getenv("OPENAQ_API_KEY")
LOCATION_ID = 12046  # Candidate station: Perungudi, Chennai

if not API_KEY:
    raise SystemExit(
        "API key missing! Check OPENAQ_API_KEY in your .env file."
    )

url = f"https://api.openaq.org/v3/locations/{LOCATION_ID}"

headers = {
    "X-API-Key": API_KEY
}

try:
    response = requests.get(
        url,
        headers=headers,
        timeout=30
    )

    print("HTTP Status:", response.status_code)

    if response.status_code != 200:
        print("API response:", response.text[:1000])
        raise SystemExit(
            "Request failed. Check the API key and station ID."
        )

    result = response.json().get("results", [])

    if not result:
        raise SystemExit("No station details returned.")

    station = result[0]

    print("\nStation:", station.get("name"))
    print("Location ID:", station.get("id"))
    print("Timezone:", station.get("timezone"))

    print("\nAvailable sensors:")

    for sensor in station.get("sensors", []):
        parameter = sensor.get("parameter", {})
        print(
            f"Sensor ID: {sensor.get('id')} | "
            f"Parameter: {parameter.get('name')} | "
            f"Units: {parameter.get('units')}"
        )

except requests.RequestException as error:
    print("Network error:", error)
