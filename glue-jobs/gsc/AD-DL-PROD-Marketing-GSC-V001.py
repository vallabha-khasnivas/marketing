import sys
import json
import boto3
import base64
import pandas as pd
from datetime import datetime, timedelta

from google.cloud import bigquery
from google.oauth2 import service_account

# ========================
# CONFIG
# ========================
SECRET_NAME = "ad-dl-prod-marketing-bigquery-secrets"
# PROJECT_ID = "gsc-as-494006"
PROJECTS = {
    "gsc-as-494006": "searchconsole",
    "gsc-export-493907": "searchconsole",
    "gsc-snf-export": "searchconsole",
    "gsc-ad-494109": "searchconsole"
}
PROJECT_MAPPING = {
    "gsc-snf-export": "sleep-n-fly",
    "gsc-ad-494109": "airportdimensions",
    "gsc-as-494006": "airport-sleepover",
    "gsc-export-493907": "theclubairportlounges"
}
DATASET = "searchconsole"

S3_BUCKET = "ad-dl-prod-rawzone"
S3_PREFIX = "marketing-projects/google-search-console/"

TABLES = [
    "searchdata_site_impression",
    "searchdata_url_impression",
    "ExportLog"
]
# ========================
# LOAD CREDENTIALS
# ========================
def get_bq_client(project_id):
    sm_client = boto3.client("secretsmanager")
    response = sm_client.get_secret_value(SecretId=SECRET_NAME)
    secret = json.loads(response["SecretString"])

    encoded_key = secret["credentials"]
    decoded_key = base64.b64decode(encoded_key)
    credentials_info = json.loads(decoded_key)

    credentials = service_account.Credentials.from_service_account_info(credentials_info)

    return bigquery.Client(credentials=credentials, project=project_id)

# ========================
# PROCESS ONE PROJECT
# ========================

def process_project(project_id, property_name, dates):
    project_id = project_id.strip()

    if not project_id:
        print("❌ Skipping empty project_id")
        return

    print(f"\n🚀 Processing: {property_name} ({project_id})")

    bq_client = get_bq_client(project_id)

    for run_date in dates:
        print(f"\n📅 Date: {run_date}")

        for table in TABLES:
            print(f"\n📊 Table: {table}")

            if table == "ExportLog":
                query = f"""
                    SELECT *
                    FROM `{project_id}.{DATASET}.{table}`
                    ORDER BY publish_time DESC
                    LIMIT 100
                """
            else:
                query = f"""
                    SELECT *
                    FROM `{project_id}.{DATASET}.{table}`
                    WHERE data_date = '{run_date}'
                """

            try:
                df = bq_client.query(query).to_dataframe()
                print(f"Fetched {len(df)} rows")
            except Exception as e:
                print(f"❌ Query failed: {str(e)}")
                continue

            if df.empty:
                print(f"⚠️ No data")
                continue

            output_path = (
                f"s3://{S3_BUCKET}/{S3_PREFIX}"
                f"{property_name}/{table}/date={run_date}/data.parquet"
            )

            import boto3

            s3 = boto3.resource('s3')
            bucket = s3.Bucket(S3_BUCKET)
            
            prefix = f"{S3_PREFIX}{property_name}/{table}/date={run_date}/"
            
            # 🔥 delete existing partition
            for obj in bucket.objects.filter(Prefix=prefix):
                obj.delete()
            df.to_parquet(output_path, index=False)
            print(f"✅ Uploaded to {output_path}")
            
# def process_project(project_id, property_name, run_date):
#     project_id = project_id.strip()   # 🔥 fix whitespace

#     if not project_id:
#         print("❌ Skipping empty project_id")
#         return

#     print(f"\n🚀 Processing: {property_name} ({project_id})")

#     bq_client = get_bq_client(project_id)

#     for table in TABLES:
#         print(f"\n📊 Table: {table}")

#         query = f"""
#             SELECT *
#             FROM `{project_id}.searchconsole.{table}`
#             WHERE data_date = '{run_date}'
#         """

#         try:
#             df = bq_client.query(query).to_dataframe()
#             print(f"Fetched {len(df)} rows")
#         except Exception as e:
#             print(f"❌ Query failed: {str(e)}")
#             continue

#         if df.empty:
#             print(f"⚠️ No data")
#             continue

#         # ✅ USE FRIENDLY NAME HERE
#         output_path = (
#             f"s3://{S3_BUCKET}/{S3_PREFIX}"
#             f"{property_name}/{table}/date={run_date}/data.parquet"
#         )

#         try:
#             df.to_parquet(output_path, index=False)
#             print(f"✅ Uploaded to {output_path}")
#         except Exception as e:
#             print(f"❌ Upload failed: {str(e)}")
# ========================
# MAIN
# ========================
# def main():
#     print("🚀 Starting Multi-Project GSC Glue Job")

#     run_date = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d")
#     print(f"Processing date: {run_date}")

#     for project_id, property_name in PROJECT_MAPPING.items():
#         process_project(project_id, property_name, run_date)

#     print("\n✅ All Projects Completed")

def main():
    print("🚀 Starting Multi-Project GSC Glue Job")

    dates = [
        (datetime.utcnow() - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(7, 0, -1)
    ]

    print(f"Processing dates: {dates}")

    for project_id, property_name in PROJECT_MAPPING.items():
        process_project(project_id, property_name, dates)

    print("\n✅ All Projects Completed")
# ========================
# ENTRY
# ========================
if __name__ == "__main__":
    main()