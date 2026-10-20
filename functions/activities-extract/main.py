"""
activities-extract
Fetch one day of activities from the AutoElite API and return as JSON.
"""

import os
import requests
import functions_framework

API_BASE = "https://btibert-bu--autoelite-api-fastapi-app.modal.run"
API_KEY  = os.environ.get("API_KEY", "")


@functions_framework.http
def task(request):

    date = request.get_json().get("date")
    if not date:
        return {"error": "date is required"}, 400

    # TODO: the API fails ~12% of the time with a 503 -- how should we handle this?
    resp = requests.get(
        f"{API_BASE}/data/activities",
        params={"date": date, "api_key": API_KEY},
        timeout=30,
    )
    resp.raise_for_status()

    return {"date": date, "records": resp.json()}, 200
