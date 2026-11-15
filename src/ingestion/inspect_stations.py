
import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("XKDR_API_KEY")

if not api_key:
    raise ValueError("XKDR_API_KEY not found in .env")

url = "https://airquality.xkdr.org/v1/stations"

headers = {
    "Authorization": f"Bearer {api_key}"
}

params = {
    "city": "Mumbai"
}

try:
    response = requests.get(
        url,
        headers=headers,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    stations = response.json()

    print("HTTP Status:", response.status_code)
    print("\nMumbai station response:\n")
    print(json.dumps(stations, indent=2))

except requests.exceptions.RequestException as error:
    print("Request failed:", error)

except ValueError as error:
    print("Could not parse response:", error)
