"""
weather-extract
A simple Cloud Function that fetches the current weather for Boston
and returns the result as JSON.
"""

from datetime import datetime, timedelta

import functions_framework
import requests


@functions_framework.http
def task(request):

    today = datetime.utcnow().strftime("%Y-%m-%d")
    # yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    # open-meteo — free, no API key required
    resp = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": 42.36,
            "longitude": -71.06,
            "current": "temperature_2m,precipitation",
            "timezone": "America/New_York",
        },
        timeout=30,
    )
    resp.raise_for_status()

    return {"date": today, "weather": resp.json()}, 200
