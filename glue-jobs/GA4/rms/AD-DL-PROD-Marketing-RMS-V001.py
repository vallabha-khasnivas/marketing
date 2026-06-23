#!/usr/bin/env python3
"""AWS Glue daily job script to export RMS reservation transactions.

Fetches yesterday's transactions from the RMS Cloud REST API using credentials from 
AWS Secrets Manager, processes and shifts them, and saves the day-wise parquet file directly to S3.
"""

import io
import json
import sys
import urllib.parse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import boto3
import pandas as pd
import requests
from awsglue.utils import getResolvedOptions

DEFAULT_REGIONS = [
    "https://restapi12.rmscloud.com",
    "https://restapi13.rmscloud.com",
    "https://restapi11.rmscloud.com",
    "https://restapi14.rmscloud.com",
    "https://restapi9.rmscloud.cn"
]

COLUMNS = [
    "res_no",
    "rms_conf_no",
    "date_made_utc",
    "arrive",
    "depart",
    "reservation_agent",
    "surname",
    "given",
    "accommodation",
    "rate",
    "date_paid",
    "property_id",
    "property_name",
    "location",
    "status",
    "booking_source_name",
    "currency",
    "created_date_utc",
    "cancelled_date_utc",
]


@dataclass(frozen=True)
class PropertyConfig:
    property_id: int
    client_name: str
    location: str


class RmsApiError(RuntimeError):
    pass


class RmsClient:
    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/") + "/"
        self.token = token
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "authtoken": self.token,
                "User-Agent": "rms-glue-job/1.0",
            }
        )

    def post(self, path: str, body: dict[str, Any], params: dict[str, Any] | None = None) -> Any:
        url = urllib.parse.urljoin(self.base_url, path.lstrip("/"))
        response = self.session.post(url, json=body, params=params, timeout=60)
        if response.status_code >= 400:
            raise RmsApiError(f"POST {url} failed with HTTP {response.status_code}: {response.text}")
        return response.json() if response.content else None

    def paged_post(
        self,
        path: str,
        body: dict[str, Any],
        params: dict[str, Any] | None = None,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        offset = 0
        while True:
            page_params = dict(params or {})
            page_params.update({"offset": offset, "limit": limit})
            page = self.post(path, body=body, params=page_params)
            rows = ensure_list(page)
            results.extend(row for row in rows if isinstance(row, dict))
            if len(rows) < limit:
                return results
            offset += limit


def ensure_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        for key in ("items", "results", "data"):
            if isinstance(value.get(key), list):
                return value[key]
    return [value]


def pick(mapping: dict[str, Any], *keys: str, default: Any = "") -> Any:
    for key in keys:
        if key in mapping and mapping[key] not in (None, ""):
            return mapping[key]
    return default


def nested_pick(mapping: dict[str, Any], *paths: tuple[str, ...], default: Any = "") -> Any:
    for path in paths:
        current: Any = mapping
        for part in path:
            if not isinstance(current, dict) or part not in current:
                current = None
                break
            current = current[part]
        if current not in (None, ""):
            return current
    return default


def first_date(value: Any) -> str:
    text = str(value or "")
    return text[:10] if len(text) >= 10 else text


def parse_and_shift(dt_str: str, shift_hours: int = -3) -> str:
    if not dt_str:
        return ""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            dt = datetime.strptime(dt_str.strip()[:19], fmt)
            adjusted = dt + timedelta(hours=shift_hours)
            return adjusted.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return dt_str.strip()[:10]


def authenticate(credentials: dict) -> tuple[str, str]:
    session = requests.Session()
    client_id = credentials["clientId"]
    base_url = None
    for region in DEFAULT_REGIONS:
        url = f"{region}/clienturl/{client_id}"
        try:
            res = session.get(url, timeout=10)
            if res.status_code == 200:
                base_url = res.text.strip()
                break
        except Exception:
            continue

    if not base_url:
        raise RuntimeError(f"Could not resolve base URL for clientId {client_id}")

    auth_url = f"{base_url.rstrip('/')}/authToken"
    payload = {
        "agentId": credentials["agentId"],
        "agentPassword": credentials["agentPassword"].strip(),
        "clientId": credentials["clientId"],
        "clientPassword": credentials["clientPassword"],
        "moduleType": credentials["moduleType"],
        "useTrainingDatabase": credentials.get("useTrainingDatabase", False)
    }

    res = session.post(auth_url, json=payload, timeout=20)
    if res.status_code not in (200, 201):
        raise RuntimeError(f"Auth failed with status {res.status_code}: {res.text}")

    token = res.json()["token"]
    return base_url, token


def get_secret(secret_name: str, region_name: str) -> dict:
    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except Exception as e:
        raise RuntimeError(f"Error fetching AWS Secret {secret_name}: {e}")


def get_properties_config(s3_client: Any, bucket: str, key: str) -> list[PropertyConfig]:
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        raw = json.loads(response["Body"].read().decode("utf-8"))
        return [
            PropertyConfig(
                property_id=int(item["propertyId"]),
                client_name=str(item["clientName"]),
                location=str(item.get("location", "")),
            )
            for item in raw
        ]
    except Exception as e:
        raise RuntimeError(f"Error reading properties config from s3://{bucket}/{key}: {e}")


def search_reservations(
    client: RmsClient,
    prop: PropertyConfig,
    start_date: str,
    end_date: str,
) -> list[dict[str, Any]]:
    body: dict[str, Any] = {
        "propertyIds": [prop.property_id],
        "includeGroupMasterReservations": "excludeGroupMasters",
        "createdFrom": f"{start_date} 00:00:00",
        "createdTo": f"{end_date} 23:59:59",
    }
    return client.paged_post(
        "/reservations/search",
        body=body,
        params={"modelType": "full"},
    )


def normalize_reservation(row: dict[str, Any], prop: PropertyConfig) -> dict[str, Any]:
    guest = row.get("primaryGuest") or row.get("guest") or {}
    
    raw_created = pick(row, "dateCreated", "createdDate")
    created_raw = parse_and_shift(raw_created, -3)
    
    arrival_raw = first_date(pick(row, "arrivalDate", "arriveDate"))
    departure_raw = first_date(pick(row, "departureDate", "departDate"))
    
    raw_cancelled = pick(row, "dateCancelled", "cancelledDate")
    cancelled_raw = parse_and_shift(raw_cancelled, -3)
    
    bs_name = pick(row, "bookingSourceName") or nested_pick(row, ("bookingSource", "name"))
    created_by = pick(row, "createdById")
    if bs_name and "sleepover" in str(bs_name).lower():
        try:
            if created_by is not None and int(float(created_by)) == 176:
                agent_name = "Sleepover.com"
            else:
                agent_name = "Sleepover e-mail/phone"
        except Exception:
            agent_name = "Sleepover e-mail/phone"
    else:
        agent_name = bs_name or pick(row, "travelAgentName") or nested_pick(row, ("travelAgent", "name"))

    return {
        "res_no": pick(row, "id", "reservationId"),
        "rms_conf_no": pick(row, "onlineConfirmationId", default="REST API"),
        "date_made_utc": created_raw,
        "arrive": arrival_raw,
        "depart": departure_raw,
        "reservation_agent": agent_name,
        "surname": pick(row, "guestSurname") or pick(guest, "lastName", "surname"),
        "given": pick(row, "guestGiven") or pick(guest, "firstName", "given"),
        "accommodation": nested_pick(row, ("categoryName",), ("category", "name")),
        "rate": "0.00",
        "date_paid": "N/A",
        "property_id": prop.property_id,
        "property_name": prop.client_name,
        "location": prop.location,
        "status": pick(row, "status"),
        "booking_source_name": pick(row, "bookingSourceName") or nested_pick(row, ("bookingSource", "name")),
        "currency": pick(row, "currency", "currencyCode", default="USD"),
        "created_date_utc": created_raw,
        "cancelled_date_utc": cancelled_raw,
    }


def main():
    # Resolve job parameters
    args = getResolvedOptions(
        sys.argv,
        [
            "secret_name",
            "s3_bucket",
            "s3_prefix",
            "properties_s3_key",
            "aws_region",
        ],
    )
    
    secret_name = args["secret_name"]
    s3_bucket = args["s3_bucket"]
    s3_prefix = args["s3_prefix"].strip("/")
    properties_s3_key = args["properties_s3_key"].strip("/")
    aws_region = args.get("aws_region", "us-east-1")

    # Define Yesterday date range (UTC)
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    print(f"Executing daily export for yesterday: {yesterday_str}")

    s3_client = boto3.client("s3")
    
    # 1. Fetch credentials and config
    credentials = get_secret(secret_name, aws_region)
    properties = get_properties_config(s3_client, s3_bucket, properties_s3_key)

    # 2. Authenticate
    base_url, token = authenticate(credentials)
    client = RmsClient(base_url, token)

    # 3. Pull transactions
    records_to_export = []
    for prop in properties:
        print(f"Fetching transactions for property: {prop.client_name}")
        # Search queries +/- 1 day around yesterday to capture shifted timezone boundaries
        search_start = (yesterday - timedelta(days=1)).strftime("%Y-%m-%d")
        search_end = (yesterday + timedelta(days=1)).strftime("%Y-%m-%d")
        
        found = search_reservations(client, prop, search_start, search_end)
        for row in found:
            norm = normalize_reservation(row, prop)
            # Filter to keep only records whose adjusted created date matches yesterday
            if norm["created_date_utc"] == yesterday_str:
                records_to_export.append(norm)

    if not records_to_export:
        print(f"No records found for {yesterday_str}. Exiting.")
        return

    # 4. Fetch rates
    all_res_ids = [int(r["res_no"]) for r in records_to_export]
    res_dates_map = {int(r["res_no"]): (r["arrive"], r["depart"]) for r in records_to_export}
    rates_map = defaultdict(float)
    chunk_size = 100
    for i in range(0, len(all_res_ids), chunk_size):
        chunk = all_res_ids[i:i+chunk_size]
        try:
            res_rates = client.post("/reservations/dailyRates/search", body={"ids": chunk})
            for rate_entry in ensure_list(res_rates):
                rid = rate_entry.get("reservationId")
                ramt = rate_entry.get("totalRateAmount")
                stay_str = rate_entry.get("stayDate")
                if rid is not None and ramt is not None and stay_str is not None:
                    rid_int = int(rid)
                    if rid_int in res_dates_map:
                        arrive_str, depart_str = res_dates_map[rid_int]
                        try:
                            dep_dt = datetime.strptime(depart_str[:10], "%Y-%m-%d")
                            buffer_dep_dt = dep_dt + timedelta(days=1)
                            dep_limit = buffer_dep_dt.strftime("%Y-%m-%d")
                        except Exception:
                            dep_limit = depart_str[:10]
                        
                        if arrive_str[:10] <= stay_str[:10] <= dep_limit:
                            rates_map[rid_int] += float(ramt)
        except Exception as e:
            print(f"Error fetching rates chunk: {e}")

    for r in records_to_export:
        rid = int(r["res_no"])
        if rid in rates_map:
            r["rate"] = f"{rates_map[rid]:.2f}"

    # 5. Write Parquet and Upload in-memory to S3
    df = pd.DataFrame(records_to_export, columns=COLUMNS)
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)
    parquet_buffer.seek(0)

    s3_key = f"{s3_prefix}/{yesterday_str}/rms.parquet"
    print(f"Uploading {len(records_to_export)} records to s3://{s3_bucket}/{s3_key}...")
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=parquet_buffer.getvalue()
    )
    print("Glue job completed successfully.")


if __name__ == "__main__":
    main()
