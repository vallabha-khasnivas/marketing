"""
AWS Glue Job: GA4 Item Revenue Daily Ingestion (Multi-Property)
Job Type: Python Shell
Python Version: Python 3.9+

Job Parameters to configure in Glue:
--additional-python-modules: PyYAML==6.0.3,pandas==3.0.3,requests==2.32.5,google-auth==2.53.0,boto3==1.43.19,pyarrow==24.0.0
--config_bucket: ad-dl-dev-sandboxzone
--ga4_config_key: google-ads/bluesky-revenue-and-visits-2dccffa28ce3.json
--start_date: (Optional, format: YYYY-MM-DD. Defaults to yesterday)
--end_date: (Optional, format: YYYY-MM-DD. Defaults to yesterday)
"""

import os
import sys
import io
import json
from datetime import datetime, timedelta
import pandas as pd
import requests
import boto3
from google.oauth2 import service_account
from google.auth.transport.requests import Request

# Initialize boto3 S3 clients
s3_client = boto3.client("s3")
s3_resource = boto3.resource("s3")

# Apply custom SSL certificates for corporate network / Zscaler (useful when testing locally)
POSSIBLE_CERTS = [
    "/Users/vallabha.khasnivas/Desktop/work/google-analytics/combined.pem",
    "/Users/vallabha.khasnivas/Desktop/work/google-analytics/ZscalerRootCertificate-2048-SHA256-Feb2025.crt",
    "/Users/vallabha.khasnivas/Desktop/work/google ads/combined-certs.pem"
]

ssl_cert_path = None
for path in POSSIBLE_CERTS:
    if os.path.exists(path):
        ssl_cert_path = path
        os.environ["AWS_CA_BUNDLE"] = path
        os.environ["SSL_CERT_FILE"] = path
        os.environ["REQUESTS_CA_BUNDLE"] = path
        os.environ["CURL_CA_BUNDLE"] = path
        print(f"Applying custom SSL certificate from: {path}")
        break

# ========================================
# 1. RESOLVE GLUE OPTIONS
# ========================================
print("Resolving job arguments...")
supported_args = [
    "JOB_NAME",
    "config_bucket",
    "ga4_config_key"
]

optional_args = ["start_date", "end_date"]
for opt in optional_args:
    if f"--{opt}" in sys.argv:
        supported_args.append(opt)

try:
    from awsglue.utils import getResolvedOptions
    args = getResolvedOptions(sys.argv, supported_args)
except ImportError:
    print("awsglue library not found, running with local testing overrides.")
    args = {
        "config_bucket": "ad-dl-dev-sandboxzone",
        "ga4_config_key": "google-ads/bluesky-revenue-and-visits-2dccffa28ce3.json"
    }
    for arg in sys.argv[1:]:
        if arg.startswith("--"):
            parts = arg.split("=")
            key = parts[0][2:]
            val = parts[1] if len(parts) > 1 else True
            args[key] = val

CONFIG_BUCKET = args.get("config_bucket", "ad-dl-dev-sandboxzone")
GA4_CONFIG_KEY = args.get("ga4_config_key", "google-ads/bluesky-revenue-and-visits-2dccffa28ce3.json")

# Calculate date range (defaulting to yesterday)
yesterday_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

user_start = args.get("start_date")
user_end = args.get("end_date")

if user_start and user_end:
    START_DATE = user_start
    END_DATE = user_end
    print(f"Using user-specified range: {START_DATE} to {END_DATE}")
else:
    START_DATE = yesterday_str
    END_DATE = yesterday_str
    print(f"Defaulting to yesterday's date: {START_DATE}")

S3_BUCKET = "ad-dl-prod-rawzone"
S3_PREFIX = "marketing-projects/ga4_revenue_by_item/revenue_by_item/"

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

PROPERTIES = {
    "theclubairportlounges": {
        "ga4_property_id": "355089312"
    },
    "sleep-n-fly": {
        "ga4_property_id": "358791306"
    }
}

OUTPUT_COLUMNS = [
    "property_name",
    "property_type",
    "data_date",
    "session_default_channel_group",
    "item_name",
    "item_revenue",
    "items_purchased"
]

# ========================================
# 2. RETRIEVE CONFIGURATION FILES FROM S3
# ========================================
print(f"Downloading GA4 credentials from s3://{CONFIG_BUCKET}/{GA4_CONFIG_KEY}...")
try:
    ga4_obj = s3_client.get_object(Bucket=CONFIG_BUCKET, Key=GA4_CONFIG_KEY)
    ga4_creds_dict = json.loads(ga4_obj["Body"].read().decode("utf-8"))
except Exception as e:
    raise Exception(f"Failed to load GA4 credentials from S3: {e}")

# ========================================
# 3. AUTHENTICATION
# ========================================
print("Authenticating with Google Analytics API...")
credentials = service_account.Credentials.from_service_account_info(
    ga4_creds_dict,
    scopes=SCOPES
)
auth_request = Request()
credentials.refresh(auth_request)
GA4_ACCESS_TOKEN = credentials.token
print("Authenticated with GA4 successfully.")

# ========================================
# HELPER FUNCTIONS
# ========================================
def run_ga4_report(property_id, dimensions, metrics, start=START_DATE, end=END_DATE):
    url = f"https://analyticsdata.googleapis.com/v1beta/properties/{property_id}:runReport"
    
    headers = {
        "Authorization": f"Bearer {GA4_ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "dateRanges": [
            {
                "startDate": start,
                "endDate": end
            }
        ],
        "dimensions": [{"name": d} for d in dimensions],
        "metrics": [{"name": m} for m in metrics],
        "limit": 100000
    }
    
    response = requests.post(
        url,
        headers=headers,
        json=payload,
        timeout=120
    )
    
    if response.status_code != 200:
        raise Exception(f"GA4 API Error (Status {response.status_code}): {response.text}")
        
    return response.json()

def delete_s3_prefix(bucket_name, prefix):
    """Deletes existing files under the specified prefix in S3."""
    bucket = s3_resource.Bucket(bucket_name)
    objects = list(bucket.objects.filter(Prefix=prefix))

    if not objects:
        return

    print(f"  Cleaning existing objects under: s3://{bucket_name}/{prefix}")
    for i in range(0, len(objects), 1000):
        batch = [{"Key": obj.key} for obj in objects[i : i + 1000]]
        bucket.delete_objects(Delete={"Objects": batch})

# ========================================
# MAIN LOOP OVER PROPERTIES
# ========================================
all_properties_records = []

for prop_name, prop_config in PROPERTIES.items():
    ga4_prop_id = prop_config["ga4_property_id"]
    
    print(f"\n========================================")
    print(f"Processing Property: {prop_name} (GA4: {ga4_prop_id})")
    print(f"========================================")
    
    dimensions = ["date", "sessionDefaultChannelGroup", "itemName"]
    metrics = ["itemRevenue", "itemsPurchased"]
    
    try:
        report_data = run_ga4_report(ga4_prop_id, dimensions, metrics)
        rows = report_data.get("rows", [])
        print(f"  Retrieved {len(rows)} rows.")
        
        for r in rows:
            date_raw = r["dimensionValues"][0]["value"]
            date_str = f"{date_raw[0:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
            channel = r["dimensionValues"][1]["value"]
            item_name = r["dimensionValues"][2]["value"]
            
            item_revenue = float(r["metricValues"][0]["value"])
            items_purchased = int(r["metricValues"][1]["value"])
            
            if item_revenue > 0 or items_purchased > 0:
                all_properties_records.append({
                    "property_name": prop_name,
                    "property_type": prop_name,
                    "data_date": date_str,
                    "session_default_channel_group": channel,
                    "item_name": item_name,
                    "item_revenue": item_revenue,
                    "items_purchased": items_purchased
                })
    except Exception as e:
        print(f"Error processing property {prop_name}: {e}")
        raise e

df_all = pd.DataFrame(all_properties_records)

if df_all.empty:
    print("\nNo data found to upload for the specified date range.")
    print("GA4 ITEM REVENUE DAILY INGESTION COMPLETED (EMPTY)")
    sys.exit(0)

# Align columns
df_all = df_all.reindex(columns=OUTPUT_COLUMNS)

# Split by date for upload
all_processed_days = {}
for date_str, group in df_all.groupby("data_date"):
    all_processed_days[date_str] = group

print(f"\nProcessed combined daywise data for {len(all_processed_days)} active days in range.")

# ========================================
# UPLOAD TO S3
# ========================================
total_item_revenue = 0
total_uploads = 0

for date_str, df_day in sorted(all_processed_days.items()):
    prefix = f"{S3_PREFIX}{date_str}/"
    s3_key = f"{prefix}data.parquet"
    
    print(f"Uploading -> s3://{S3_BUCKET}/{s3_key}")
    
    # 1. Clear old file in the prefix
    delete_s3_prefix(S3_BUCKET, prefix)
    
    # 2. Write Parquet in memory
    buffer = io.BytesIO()
    df_day.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    # 3. Put to S3
    s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
    
    day_revenue = df_day["item_revenue"].sum()
    print(f"  Uploaded {len(df_day)} rows (Revenue: ${day_revenue:,.2f})")
    
    total_item_revenue += day_revenue
    total_uploads += 1

print("\n========================================")
print("GA4 ITEM REVENUE S3 DAILY INGESTION COMPLETED")
print(f"Total uploaded files: {total_uploads}")
print(f"Total Revenue processed: ${total_item_revenue:,.2f}")
print("========================================")
