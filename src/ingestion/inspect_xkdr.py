import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Read the API key
api_key = os.getenv("XKDR_API_KEY")

if not api_key:
    raise ValueError(
        "XKDR_API_KEY not found. Check your .env file."
    )

# XKDR metadata endpoint
url = "https://airquality.xkdr.org/v1/meta"

# Authentication header
headers = {
    "Authorization": f"Bearer {api_key}"
}

try:
    response = requests.get(
        url,
        headers=headers,
        timeout=30
    )

    # Raise an error for unsuccessful HTTP responses
    response.raise_for_status()

    # Convert JSON response into Python data
    metadata = response.json()

    print("\nConnection successful!")
    print("HTTP Status:", response.status_code)
    print("\nXKDR Metadata:")
    print(metadata)

except requests.exceptions.HTTPError as error:
    print("HTTP error:", error)
    print("Status code:", response.status_code)

except requests.exceptions.RequestException as error:
    print("Connection error:", error)

except ValueError as error:
    print("Configuration or JSON error:", error)