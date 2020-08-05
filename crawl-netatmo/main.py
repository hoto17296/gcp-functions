import os
import json
import urllib.request
from datetime import datetime
from oauthlib.oauth2 import WebApplicationClient
import pandas as pd
from google.cloud import bigquery


class NetatmoDevice:
    def __init__(self, client_id, client_secret, refresh_token, device_id, api_base="https://api.netatmo.com"):
        self.device_id = device_id
        self.api_base = api_base
        self.access_token = self._get_access_token(client_id, client_secret, refresh_token)

    def _get_access_token(self, client_id, client_secret, refresh_token):
        oauth = WebApplicationClient(client_id, refresh_token=refresh_token)
        url, headers, body = oauth.prepare_refresh_token_request(
            f"{self.api_base}/oauth2/token", client_id=client_id, client_secret=client_secret
        )
        req = urllib.request.Request(url, body.encode(), headers=headers)
        with urllib.request.urlopen(req) as res:
            oauth.parse_request_body_response(res.read())
        return oauth.access_token

    def get_measure(self, date_begin=None, columns=["temperature", "co2", "humidity", "pressure", "noise"]):
        headers = {
            "Authorization": f"Bearer {self.access_token}",
        }
        params = {
            "date_begin": int(date_begin.timestamp() + 150) if date_begin is not None else None,
            "device_id": self.device_id,
            "scale": "5min",
            "type": ",".join(columns),
            "optimize": False,
        }
        url = f"{self.api_base}/api/getmeasure?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req) as res:
            data = json.load(res)["body"]
        df = pd.DataFrame(data, index=columns).T
        df.index = pd.to_datetime(df.index, unit="s")
        return df


def handler(event, context):
    table = "hotolab.netatmo.indoor"
    client = bigquery.Client()
    (row,) = client.query(f"SELECT MAX(ts) FROM {table}").result()
    latest_ts = row[0]
    device = NetatmoDevice(
        os.environ.get("NETATMO_API_CLIENT_ID"),
        os.environ.get("NETATMO_API_CLIENT_SECRET"),
        os.environ.get("NETATMO_API_REFRESH_TOKEN"),
        os.environ.get("NETATMO_DEVICE_ID"),
    )
    df = device.get_measure(latest_ts)
    if len(df) > 0:
        df.index.name = "ts"
        client.load_table_from_dataframe(df.reset_index(), table).result()
