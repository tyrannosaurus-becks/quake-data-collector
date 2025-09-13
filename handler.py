import boto3
from datetime import date, timedelta
import io
import json
from os import getenv
from urllib3 import PoolManager


def lambda_handler(event, context):
    hours_ago = int(getenv("HOURS_AGO"))
    min_latitude = getenv("MIN_LATITUDE")
    min_longitude = getenv("MIN_LONGITUDE")
    max_latitude = getenv("MAX_LATITUDE")
    max_longitude = getenv("MAX_LONGITUDE")
    s3_bucket = getenv("S3_BUCKET")

    seismic_activity = get_seismic_activity(hours_ago, min_latitude, min_longitude, max_latitude, max_longitude)

    s3 = boto3.client('s3')
    for event in seismic_activity:
        event_id = extract_id(event["properties"]["url"])
        event_bytes = io.BytesIO(json.dumps(event).encode('utf-8'))
        s3.put_object(
            Bucket=s3_bucket,
            Key=f"raw/usgs/{event_id}",
            Body=event_bytes,
            ContentType='application/json'
        )


def extract_id(url):
    url_fields = url.split("/")
    return url_fields[-1]


# This returns quakes and explosions.
def get_seismic_activity(hours_ago, min_latitude, min_longitude, max_latitude, max_longitude):
    # API docs are located here:
    # https://earthquake.usgs.gov/fdsnws/event/1/#parameters
    query_params = {
        "format": "geojson",
        "updatedafter": format_date(date.today() - timedelta(hours=hours_ago)),
        "minlatitude": min_latitude,
        "minlongitude": min_longitude,
        "maxlatitude": max_latitude,
        "maxlongitude": max_longitude,
    }

    request_url = "https://earthquake.usgs.gov/fdsnws/event/1/query?"
    for key, value in query_params.items():
        request_url += f"&{key}={value}"

    http = PoolManager()
    resp = http.request("GET", request_url)
    if resp.status != 200:
        raise Exception(resp.status)
    return json.loads(resp.data.decode('utf-8'))["features"]


def format_date(dt):
    return dt.strftime("%Y-%m-%d")


if __name__ == "__main__":
    lambda_handler({}, {})
