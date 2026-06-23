"""
AWS Glue Job: Google Ads & GA4 Unified Daily Ingestion (Multi-Property)
Job Type: Python Shell
Python Version: Python 3.9+

Job Parameters to configure in Glue:
--additional-python-modules: PyYAML==6.0.3,pandas==3.0.3,requests==2.32.5,google-auth==2.53.0,boto3==1.43.19,pyarrow==24.0.0
--config_bucket: ad-dl-prod-rawzone
--gads_config_key: marketing-projects/google-ads/config/google-ads.yaml
--ga4_config_key: marketing-projects/google-ads/config/bluesky-revenue-and-visits-2dccffa28ce3.json
--start_date: (Optional, format: YYYY-MM-DD. Defaults to 1st of yesterday's month)
--end_date: (Optional, format: YYYY-MM-DD. Defaults to yesterday's date)
"""

import os
import sys
import io
import json
from datetime import datetime, timedelta
import yaml
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
    "gads_config_key",
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
        "config_bucket": "ad-dl-prod-rawzone",
        "gads_config_key": "marketing-projects/google-ads/config/google-ads.yaml",
        "ga4_config_key": "marketing-projects/google-ads/config/bluesky-revenue-and-visits-2dccffa28ce3.json"
    }
    for arg in sys.argv[1:]:
        if arg.startswith("--"):
            parts = arg.split("=")
            key = parts[0][2:]
            val = parts[1] if len(parts) > 1 else True
            args[key] = val

CONFIG_BUCKET = args.get("config_bucket", "ad-dl-prod-rawzone")
GADS_CONFIG_KEY = args.get("gads_config_key", "marketing-projects/google-ads/config/google-ads.yaml")
GA4_CONFIG_KEY = args.get("ga4_config_key", "marketing-projects/google-ads/config/bluesky-revenue-and-visits-2dccffa28ce3.json")

# Calculate date range (defaulting to month-to-date of yesterday)
yesterday_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

user_start = args.get("start_date")
user_end = args.get("end_date")

if user_start and user_end:
    START_DATE = user_start
    END_DATE = user_end
    print(f"Using user-specified range: {START_DATE} to {END_DATE}")
else:
    target_dt = datetime.strptime(yesterday_str, "%Y-%m-%d")
    START_DATE = target_dt.replace(day=1).strftime("%Y-%m-%d")
    END_DATE = yesterday_str
    print(f"Defaulting to month-to-date range: {START_DATE} to {END_DATE}")

S3_BUCKET = "ad-dl-prod-rawzone"
S3_PREFIX = "marketing-projects/google-ads/"

SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

PROPERTIES = {
    "theclubairportlounges": {
        "ga4_property_id": "355089312",
        "gads_customer_id": "5979083047"
    },
    "sleep-n-fly": {
        "ga4_property_id": "358791306",
        "gads_customer_id": "5471508358"
    }
}

OUTPUT_COLUMNS = [
    "property_name",
    "property_type",
    "data_date",
    "campaign",
    "ad_group",
    "session_default_channel_group",
    "session_medium",
    "cost",
    "revenue",
    "roas",
    "clicks",
    "impressions",
    "ctr",
    "avg_cpc",
    "conversions",
    "sessions",
    "revenue_ga4",
    "conversions_ga4",
    "roas_ga4",
    "sessions_ga4"
]

# ========================================
# 2. RETRIEVE CONFIGURATION FILES FROM S3
# ========================================
print(f"Downloading Google Ads config from s3://{CONFIG_BUCKET}/{GADS_CONFIG_KEY}...")
try:
    gads_obj = s3_client.get_object(Bucket=CONFIG_BUCKET, Key=GADS_CONFIG_KEY)
    gads_config = yaml.safe_load(gads_obj["Body"].read().decode("utf-8"))
except Exception as e:
    raise Exception(f"Failed to load Google Ads config from S3: {e}")

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

print("Authenticating with Google Ads API (REST OAuth)...")
token_url = "https://oauth2.googleapis.com/token"
payload = {
    "client_id": gads_config["client_id"],
    "client_secret": gads_config["client_secret"],
    "refresh_token": gads_config["refresh_token"],
    "grant_type": "refresh_token"
}
response = requests.post(token_url, data=payload, verify=ssl_cert_path if ssl_cert_path else True)
if response.status_code != 200:
    raise Exception(f"Failed to refresh Google Ads OAuth token: {response.text}")
gads_access_token = response.json()["access_token"]

api_version = "v24"

gads_headers = {
    "Authorization": f"Bearer {gads_access_token}",
    "developer-token": gads_config["developer_token"],
    "Content-Type": "application/json"
}
if gads_config.get("login_customer_id"):
    gads_headers["login-customer-id"] = str(gads_config["login_customer_id"])

print("Authenticated with Google Ads successfully.")

# ========================================
# HELPER FUNCTIONS
# ========================================
def get_default_channel(campaign_name):
    """Deterministically map campaign to PMax or Paid Search default channels."""
    cn = campaign_name.lower()
    if "performance max" in cn or "pmax" in cn or "performance_max" in cn:
        return "Cross-network"
    return "Paid Search"

def run_google_ads_report(cust_id, query):
    url = f"https://googleads.googleapis.com/{api_version}/customers/{cust_id}/googleAds:search"
    results = []
    payload = {
        "query": query
    }
    while True:
        response = requests.post(
            url,
            headers=gads_headers,
            json=payload,
            verify=ssl_cert_path if ssl_cert_path else True,
            timeout=120
        )
        if response.status_code != 200:
            raise Exception(f"Google Ads API Error (Status {response.status_code}): {response.text}")
        res_json = response.json()
        results.extend(res_json.get("results", []))
        next_token = res_json.get("nextPageToken")
        if not next_token:
            break
        payload["pageToken"] = next_token
    return results

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
        verify=ssl_cert_path if ssl_cert_path else True,
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
all_properties_merged = []

for prop_name, prop_config in PROPERTIES.items():
    ga4_prop_id = prop_config["ga4_property_id"]
    gads_cust_id = prop_config["gads_customer_id"]
    
    print(f"\n========================================")
    print(f"Processing Property: {prop_name} (GA4: {ga4_prop_id}, Ads: {gads_cust_id})")
    print(f"========================================")
    
    # 1. Fetch Google Ads Native Data
    print("1. Fetching Google Ads Native Data via REST API...")
    query_campaign = f"""
        SELECT
            segments.date,
            campaign.name,
            metrics.cost_micros,
            metrics.clicks,
            metrics.impressions,
            metrics.conversions,
            metrics.conversions_value
        FROM campaign
        WHERE segments.date BETWEEN '{START_DATE}' AND '{END_DATE}'
    """

    query_adgroup = f"""
        SELECT
            segments.date,
            campaign.name,
            ad_group.name,
            metrics.cost_micros,
            metrics.clicks,
            metrics.impressions,
            metrics.conversions,
            metrics.conversions_value
        FROM ad_group
        WHERE segments.date BETWEEN '{START_DATE}' AND '{END_DATE}'
    """

    print("  Querying Google Ads campaigns...")
    gads_camp_rows = run_google_ads_report(gads_cust_id, query_campaign)
    print(f"  Retrieved {len(gads_camp_rows)} campaign-date rows.")

    gads_campaigns = {}
    for row in gads_camp_rows:
        date_str = row.get("segments", {}).get("date")
        camp = row.get("campaign", {}).get("name", "N/A")
        metrics = row.get("metrics", {})
        cost = float(metrics.get("costMicros", 0)) / 1000000.0
        clicks = int(metrics.get("clicks", 0))
        impressions = int(metrics.get("impressions", 0))
        conversions = float(metrics.get("conversions", 0.0))
        revenue = float(metrics.get("conversionsValue", 0.0))
        
        if date_str not in gads_campaigns:
            gads_campaigns[date_str] = {}
        gads_campaigns[date_str][camp] = {
            "cost": cost,
            "clicks": clicks,
            "impressions": impressions,
            "conversions": conversions,
            "revenue": revenue
        }

    print("  Querying Google Ads ad groups...")
    gads_adgroup_rows = run_google_ads_report(gads_cust_id, query_adgroup)
    print(f"  Retrieved {len(gads_adgroup_rows)} adgroup-date rows.")

    gads_adgroups = []
    for row in gads_adgroup_rows:
        date_str = row.get("segments", {}).get("date")
        camp = row.get("campaign", {}).get("name", "N/A")
        adg = row.get("adGroup", {}).get("name", "N/A")
        metrics = row.get("metrics", {})
        cost = float(metrics.get("costMicros", 0)) / 1000000.0
        clicks = int(metrics.get("clicks", 0))
        impressions = int(metrics.get("impressions", 0))
        conversions = float(metrics.get("conversions", 0.0))
        revenue = float(metrics.get("conversionsValue", 0.0))
        
        gads_adgroups.append({
            "date": date_str,
            "campaign": camp,
            "ad_group": adg,
            "cost": cost,
            "clicks": clicks,
            "impressions": impressions,
            "conversions": conversions,
            "revenue": revenue
        })

    df_gads_adgroup = pd.DataFrame(gads_adgroups)

    # Process Google Ads daily hybrid merge
    gads_records = []
    for date_str, campaigns_on_date in gads_campaigns.items():
        df_gads_adgroup_on_date = df_gads_adgroup[df_gads_adgroup["date"] == date_str] if not df_gads_adgroup.empty else pd.DataFrame()
        
        for campaign, camp_metrics in campaigns_on_date.items():
            df_camp_adgroups = df_gads_adgroup_on_date[df_gads_adgroup_on_date["campaign"] == campaign] if not df_gads_adgroup_on_date.empty else pd.DataFrame()
            
            if not df_camp_adgroups.empty:
                for _, row in df_camp_adgroups.iterrows():
                    gads_records.append({
                        "data_date": date_str,
                        "campaign": row["campaign"],
                        "ad_group": row["ad_group"],
                        "cost": row["cost"],
                        "clicks": row["clicks"],
                        "impressions": row["impressions"],
                        "conversions": row["conversions"],
                        "revenue": row["revenue"]
                    })
            else:
                gads_records.append({
                    "data_date": date_str,
                    "campaign": campaign,
                    "ad_group": "(campaign level)",
                    "cost": camp_metrics["cost"],
                    "clicks": camp_metrics["clicks"],
                    "impressions": camp_metrics["impressions"],
                    "conversions": camp_metrics["conversions"],
                    "revenue": camp_metrics["revenue"]
                })

    df_gads_daily = pd.DataFrame(gads_records)
    if df_gads_daily.empty:
        df_gads_daily = pd.DataFrame(columns=["data_date", "campaign", "ad_group", "cost", "clicks", "impressions", "conversions", "revenue"])

    # 2. Fetch GA4 Reconciled Data (split by dimension compatibility)
    print("2. Fetching GA4 Reconciled Data...")
    
    # Query monthly targets
    print("  Querying monthly unthresholded targets (Google Ads)...")
    monthly_gads_data = run_ga4_report(ga4_prop_id, ["googleAdsCampaignName", "googleAdsAdGroupName"], ["advertiserAdCost", "advertiserAdClicks", "advertiserAdImpressions"])
    print("  Querying monthly unthresholded targets (GA4)...")
    monthly_ga4_data = run_ga4_report(ga4_prop_id, ["googleAdsCampaignName", "googleAdsAdGroupName", "sessionDefaultChannelGroup", "sessionMedium"], ["purchaseRevenue", "conversions", "sessions"])

    campaign_targets = {}
    
    # Build from monthly GA4 report
    for row in monthly_ga4_data.get("rows", []):
        camp = row["dimensionValues"][0]["value"]
        adg = row["dimensionValues"][1]["value"]
        chan = row["dimensionValues"][2]["value"]
        med = row["dimensionValues"][3]["value"]
        if camp == "(not set)" or not camp.strip():
            continue
        met_vals = row["metricValues"]
        revenue = float(met_vals[0].get("value", 0))
        conversions = float(met_vals[1].get("value", 0))
        sessions = int(met_vals[2].get("value", 0))
        
        if camp not in campaign_targets:
            campaign_targets[camp] = {}
        if adg not in campaign_targets[camp]:
            campaign_targets[camp][adg] = {}
        campaign_targets[camp][adg][(chan, med)] = {
            "cost": 0.0, "clicks": 0, "impressions": 0,
            "revenue": revenue, "conversions": conversions, "sessions": sessions
        }

    # Overlay advertiser parameters onto matching default channels
    for row in monthly_gads_data.get("rows", []):
        camp = row["dimensionValues"][0]["value"]
        adg = row["dimensionValues"][1]["value"]
        if camp == "(not set)" or not camp.strip():
            continue
        met_vals = row["metricValues"]
        cost = float(met_vals[0].get("value", 0))
        clicks = int(met_vals[1].get("value", 0))
        impressions = int(met_vals[2].get("value", 0))
        chan = get_default_channel(camp)
        med = "cpc"
        
        if camp not in campaign_targets:
            campaign_targets[camp] = {}
        if adg not in campaign_targets[camp]:
            campaign_targets[camp][adg] = {}
        if (chan, med) not in campaign_targets[camp][adg]:
            campaign_targets[camp][adg][(chan, med)] = {
                "cost": 0.0, "clicks": 0, "impressions": 0,
                "revenue": 0.0, "conversions": 0.0, "sessions": 0
            }
        campaign_targets[camp][adg][(chan, med)]["cost"] = cost
        campaign_targets[camp][adg][(chan, med)]["clicks"] = clicks
        campaign_targets[camp][adg][(chan, med)]["impressions"] = impressions

    search_campaign_targets = {}
    for camp, adgroups in campaign_targets.items():
        search_campaign_targets[camp] = {}
        for adg, channels in adgroups.items():
            if adg not in ["(not set)", ""]:
                search_campaign_targets[camp][adg] = channels

    # Query daily reports
    print("  Querying daily GA4 campaign-level report (Google Ads)...")
    daily_camp_gads = run_ga4_report(ga4_prop_id, ["date", "googleAdsCampaignName"], ["advertiserAdCost", "advertiserAdClicks", "advertiserAdImpressions"])
    print("  Querying daily GA4 campaign-level report (GA4)...")
    daily_camp_ga4 = run_ga4_report(ga4_prop_id, ["date", "googleAdsCampaignName", "sessionDefaultChannelGroup", "sessionMedium"], ["purchaseRevenue", "conversions", "sessions"])
    
    print("  Querying daily GA4 adgroup-level report (Google Ads)...")
    daily_adgroup_gads = run_ga4_report(ga4_prop_id, ["date", "googleAdsCampaignName", "googleAdsAdGroupName"], ["advertiserAdCost", "advertiserAdClicks", "advertiserAdImpressions"])
    print("  Querying daily GA4 adgroup-level report (GA4)...")
    daily_adgroup_ga4 = run_ga4_report(ga4_prop_id, ["date", "googleAdsCampaignName", "googleAdsAdGroupName", "sessionDefaultChannelGroup", "sessionMedium"], ["purchaseRevenue", "conversions", "sessions"])

    # Process daily campaign reports
    campaigns_dict = {}
    for row in daily_camp_ga4.get("rows", []):
        date_raw = row["dimensionValues"][0]["value"]
        date_str = f"{date_raw[0:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
        camp = row["dimensionValues"][1]["value"]
        chan = row["dimensionValues"][2]["value"]
        med = row["dimensionValues"][3]["value"]
        met_vals = row["metricValues"]
        
        if date_str not in campaigns_dict:
            campaigns_dict[date_str] = {}
        if camp not in campaigns_dict[date_str]:
            campaigns_dict[date_str][camp] = {}
        campaigns_dict[date_str][camp][(chan, med)] = {
            "cost": 0.0, "clicks": 0, "impressions": 0,
            "revenue": float(met_vals[0].get("value", 0)),
            "conversions": float(met_vals[1].get("value", 0)),
            "sessions": int(met_vals[2].get("value", 0))
        }

    for row in daily_camp_gads.get("rows", []):
        date_raw = row["dimensionValues"][0]["value"]
        date_str = f"{date_raw[0:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
        camp = row["dimensionValues"][1]["value"]
        chan = get_default_channel(camp)
        med = "cpc"
        met_vals = row["metricValues"]
        
        if date_str not in campaigns_dict:
            campaigns_dict[date_str] = {}
        if camp not in campaigns_dict[date_str]:
            campaigns_dict[date_str][camp] = {}
        if (chan, med) not in campaigns_dict[date_str][camp]:
            campaigns_dict[date_str][camp][(chan, med)] = {
                "cost": 0.0, "clicks": 0, "impressions": 0,
                "revenue": 0.0, "conversions": 0.0, "sessions": 0
            }
        campaigns_dict[date_str][camp][(chan, med)]["cost"] = float(met_vals[0].get("value", 0))
        campaigns_dict[date_str][camp][(chan, med)]["clicks"] = int(met_vals[1].get("value", 0))
        campaigns_dict[date_str][camp][(chan, med)]["impressions"] = int(met_vals[2].get("value", 0))

    # Process daily adgroup reports
    adgroup_dict = {}
    for row in daily_adgroup_ga4.get("rows", []):
        date_raw = row["dimensionValues"][0]["value"]
        date_str = f"{date_raw[0:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
        camp = row["dimensionValues"][1]["value"]
        adg = row["dimensionValues"][2]["value"]
        chan = row["dimensionValues"][3]["value"]
        med = row["dimensionValues"][4]["value"]
        met_vals = row["metricValues"]
        
        key = (date_str, camp, adg, chan, med)
        adgroup_dict[key] = {
            "cost": 0.0, "clicks": 0, "impressions": 0,
            "revenue": float(met_vals[0].get("value", 0)),
            "conversions": float(met_vals[1].get("value", 0)),
            "sessions": int(met_vals[2].get("value", 0))
        }

    for row in daily_adgroup_gads.get("rows", []):
        date_raw = row["dimensionValues"][0]["value"]
        date_str = f"{date_raw[0:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
        camp = row["dimensionValues"][1]["value"]
        adg = row["dimensionValues"][2]["value"]
        chan = get_default_channel(camp)
        med = "cpc"
        met_vals = row["metricValues"]
        
        key = (date_str, camp, adg, chan, med)
        if key not in adgroup_dict:
            adgroup_dict[key] = {
                "cost": 0.0, "clicks": 0, "impressions": 0,
                "revenue": 0.0, "conversions": 0.0, "sessions": 0
            }
        adgroup_dict[key]["cost"] = float(met_vals[0].get("value", 0))
        adgroup_dict[key]["clicks"] = int(met_vals[1].get("value", 0))
        adgroup_dict[key]["impressions"] = int(met_vals[2].get("value", 0))

    adgroup_list = []
    for (date_str, camp, adg, chan, med), metrics_dict in adgroup_dict.items():
        adgroup_list.append({
            "date": date_str, "campaign": camp, "ad_group": adg, "channel": chan, "medium": med,
            **metrics_dict
        })

    df_adgroup = pd.DataFrame(adgroup_list)

    daily_unthresholded_sums = {}
    for row in adgroup_list:
        camp = row["campaign"]
        adg = row["ad_group"]
        chan = row["channel"]
        med = row["medium"]
        if camp not in search_campaign_targets or adg not in search_campaign_targets[camp]:
            continue
        if camp not in daily_unthresholded_sums:
            daily_unthresholded_sums[camp] = {}
        if adg not in daily_unthresholded_sums[camp]:
            daily_unthresholded_sums[camp][adg] = {}
        if (chan, med) not in daily_unthresholded_sums[camp][adg]:
            daily_unthresholded_sums[camp][adg][(chan, med)] = {m: 0.0 for m in ["cost", "clicks", "impressions", "revenue", "conversions", "sessions"]}
        for m in ["cost", "clicks", "impressions", "revenue", "conversions", "sessions"]:
            daily_unthresholded_sums[camp][adg][(chan, med)][m] += row[m]

    discrepancy_weights = {}
    for camp, adgroups in search_campaign_targets.items():
        discrepancy_weights[camp] = {}
        for adg, channels in adgroups.items():
            discrepancy_weights[camp][adg] = {}
            for (chan, med), target_metrics in channels.items():
                discrepancy_weights[camp][adg][(chan, med)] = {}
                for m in ["cost", "clicks", "impressions", "revenue", "conversions", "sessions"]:
                    target_val = target_metrics[m]
                    unthresholded_val = daily_unthresholded_sums.get(camp, {}).get(adg, {}).get((chan, med), {}).get(m, 0.0)
                    discrepancy_weights[camp][adg][(chan, med)][m] = max(0.0, target_val - unthresholded_val)

    carry_overs = {}
    for camp, adgroups in search_campaign_targets.items():
        carry_overs[camp] = {}
        for adg, channels in adgroups.items():
            carry_overs[camp][adg] = {}
            for (chan, med) in channels.keys():
                carry_overs[camp][adg][(chan, med)] = {
                    "clicks": 0.0,
                    "impressions": 0.0,
                    "sessions": 0.0
                }

    def distribute_metric_with_carry(total_val, weights, carry_over_dict, is_integer=False):
        if not weights: return {}
        tot_weight = sum(weights.values())
        if tot_weight <= 0:
            proportions = {k: 1.0 / len(weights) for k in weights.keys()}
        else:
            proportions = {k: w / tot_weight for k, w in weights.items()}
        if len(proportions) == 1: return {k: total_val for k in proportions.keys()}
        
        distributed = {}
        remaining_val = total_val
        sorted_items = sorted(proportions.items(), key=lambda x: x[1], reverse=True)
        for i, (adg, prop) in enumerate(sorted_items):
            if i == len(sorted_items) - 1:
                distributed[adg] = remaining_val
                if is_integer:
                    carry_over_dict[adg] = (total_val * prop + carry_over_dict.get(adg, 0.0)) - remaining_val
            else:
                if is_integer:
                    exact = total_val * prop + carry_over_dict.get(adg, 0.0)
                    allocated = int(round(exact))
                    carry_over_dict[adg] = exact - allocated
                else:
                     allocated = round(total_val * prop, 6)
                distributed[adg] = allocated
                remaining_val -= allocated
        return distributed

    ga4_records = []
    for date_str in sorted(campaigns_dict.keys()):
        df_adgroup_on_date = df_adgroup[df_adgroup["date"] == date_str] if not df_adgroup.empty else pd.DataFrame()
        for campaign, channels in campaigns_dict[date_str].items():
            if campaign == "(not set)" or not campaign.strip(): continue
            
            is_pmax = campaign not in search_campaign_targets or not search_campaign_targets[campaign]
            
            for (chan, med), camp_metrics in channels.items():
                if not (camp_metrics["cost"] > 0 or camp_metrics["clicks"] > 0 or camp_metrics["impressions"] > 0 or camp_metrics["revenue"] > 0 or camp_metrics["sessions"] > 0): continue
                
                if is_pmax:
                    ga4_records.append({
                        "data_date": date_str,
                        "campaign": campaign,
                        "ad_group": "(campaign level)",
                        "session_default_channel_group": chan,
                        "session_medium": med,
                        "revenue_ga4": camp_metrics["revenue"],
                        "conversions_ga4": camp_metrics["conversions"],
                        "sessions_ga4": camp_metrics["sessions"]
                    })
                    continue
                
                df_camp_adgroups = df_adgroup_on_date[(df_adgroup_on_date["campaign"] == campaign) & (df_adgroup_on_date["channel"] == chan) & (df_adgroup_on_date["medium"] == med)] if not df_adgroup_on_date.empty else pd.DataFrame()
                day_adgroup_metrics = {}
                for _, row in df_camp_adgroups.iterrows():
                    day_adgroup_metrics[row["ad_group"]] = {
                        "revenue": row["revenue"], "conversions": row["conversions"], "sessions": row["sessions"]
                    }
                    
                adgroup_total_revenue = df_camp_adgroups["revenue"].sum() if not df_camp_adgroups.empty else 0
                adgroup_total_conversions = df_camp_adgroups["conversions"].sum() if not df_camp_adgroups.empty else 0
                adgroup_total_sessions = df_camp_adgroups["sessions"].sum() if not df_camp_adgroups.empty else 0
                
                diff_revenue = max(0.0, camp_metrics["revenue"] - adgroup_total_revenue)
                diff_conversions = max(0.0, camp_metrics["conversions"] - adgroup_total_conversions)
                diff_sessions = max(0, camp_metrics["sessions"] - adgroup_total_sessions)
                
                weights_dict = discrepancy_weights.get(campaign, {})
                rev_weights = {adg: weights_dict[adg][(chan, med)]["revenue"] for adg in weights_dict if (chan, med) in weights_dict[adg]}
                conv_weights = {adg: weights_dict[adg][(chan, med)]["conversions"] for adg in weights_dict if (chan, med) in weights_dict[adg]}
                sess_weights = {adg: weights_dict[adg][(chan, med)]["sessions"] for adg in weights_dict if (chan, med) in weights_dict[adg]}
                
                carry_overs_dict = carry_overs.get(campaign, {})
                sess_carry = {adg: carry_overs_dict[adg][(chan, med)]["sessions"] for adg in carry_overs_dict if (chan, med) in carry_overs_dict[adg]}
                
                diff_rev_dist = distribute_metric_with_carry(diff_revenue, rev_weights, {}, is_integer=False)
                diff_conv_dist = distribute_metric_with_carry(diff_conversions, conv_weights, {}, is_integer=False)
                diff_sess_dist = distribute_metric_with_carry(diff_sessions, sess_weights, sess_carry, is_integer=True)
                
                # Update carry overs
                for adg, val in sess_carry.items():
                    if campaign in carry_overs and adg in carry_overs[campaign] and (chan, med) in carry_overs[campaign][adg]:
                        carry_overs[campaign][adg][(chan, med)]["sessions"] = val
                
                for adg in search_campaign_targets[campaign].keys():
                    if (chan, med) in search_campaign_targets[campaign][adg]:
                        orig = day_adgroup_metrics.get(adg, {"revenue": 0.0, "conversions": 0.0, "sessions": 0})
                        ga4_records.append({
                            "data_date": date_str,
                            "campaign": campaign,
                            "ad_group": adg,
                            "session_default_channel_group": chan,
                            "session_medium": med,
                            "revenue_ga4": orig["revenue"] + diff_rev_dist.get(adg, 0.0),
                            "conversions_ga4": orig["conversions"] + diff_conv_dist.get(adg, 0.0),
                            "sessions_ga4": orig["sessions"] + diff_sess_dist.get(adg, 0)
                        })

    df_ga4_daily = pd.DataFrame(ga4_records)
    if df_ga4_daily.empty:
        df_ga4_daily = pd.DataFrame(columns=["data_date", "campaign", "ad_group", "session_default_channel_group", "session_medium", "revenue_ga4", "conversions_ga4", "sessions_ga4"])

    # 3. Merge Google Ads Native + GA4 with metric splitting
    print("3. Merging Google Ads Native with Reconciled GA4...")
    
    # Calculate session weights to split Google Ads cost/clicks/impressions proportionally
    ga4_sums = df_ga4_daily.groupby(["data_date", "campaign", "ad_group"])["sessions_ga4"].sum().reset_index()
    ga4_sums.rename(columns={"sessions_ga4": "total_sessions_ga4"}, inplace=True)
    
    df_ga4_weighted = pd.merge(df_ga4_daily, ga4_sums, on=["data_date", "campaign", "ad_group"], how="left")
    df_ga4_weighted["session_weight"] = df_ga4_weighted["sessions_ga4"] / df_ga4_weighted["total_sessions_ga4"]
    df_ga4_weighted["session_weight"] = df_ga4_weighted["session_weight"].fillna(1.0 / df_ga4_weighted.groupby(["data_date", "campaign", "ad_group"])["session_default_channel_group"].transform("count"))
    df_ga4_weighted["session_weight"] = df_ga4_weighted["session_weight"].fillna(1.0)
    
    df_merged = pd.merge(df_gads_daily, df_ga4_weighted, on=["data_date", "campaign", "ad_group"], how="left")
    
    # Distribute native metrics by weights
    for col in ["cost", "clicks", "impressions","conversions", "revenue"]:
        df_merged[col] = df_merged[col] * df_merged["session_weight"].fillna(1.0)
        
    # Default missing channels (if GA4 didn't track any sessions for a Google Ads row)
    is_pmax_series = df_merged["campaign"].str.lower().str.contains("performance max|pmax|performance_max", na=False)
    df_merged["session_default_channel_group"] = df_merged["session_default_channel_group"].fillna(
        pd.Series(map(lambda x: "Cross-network" if x else "Paid Search", is_pmax_series), index=df_merged.index)
    )
    df_merged["session_medium"] = df_merged["session_medium"].fillna("cpc")
    
    if "session_weight" in df_merged.columns:
        df_merged.drop(columns=["session_weight", "total_sessions_ga4"], inplace=True)

    # Set NaN values to 0
    for col in ["revenue_ga4", "conversions_ga4", "sessions_ga4"]:
        df_merged[col] = df_merged[col].fillna(0.0)

    # Convert GA4 revenue from USD to GBP for sleep-n-fly to match Google Ads GBP currency
    if prop_name == "sleep-n-fly":
        df_merged["revenue_ga4"] = df_merged["revenue_ga4"] / 1.3492

    # Set sessions to sessions_ga4
    df_merged["sessions"] = df_merged["sessions_ga4"]

    # Fill standard columns
    for col in ["cost", "clicks", "impressions", "conversions", "revenue"]:
        df_merged[col] = df_merged[col].fillna(0.0)

    # Add property name and type columns
    df_merged["property_name"] = prop_name
    df_merged["property_type"] = prop_name

    all_properties_merged.append(df_merged)


# ========================================
# CONCAT AND CALCULATE DERIVED METRICS
# ========================================
print("\nCombining results for all properties...")
df_all_properties = pd.concat(all_properties_merged, ignore_index=True)

# Calculate standard derived metrics
df_all_properties["roas"] = (df_all_properties["revenue"] / df_all_properties["cost"]).fillna(0.0)
df_all_properties["ctr"] = (df_all_properties["clicks"] / df_all_properties["impressions"] * 100).fillna(0.0)
df_all_properties["avg_cpc"] = (df_all_properties["cost"] / df_all_properties["clicks"]).fillna(0.0)

# Calculate GA4 ROAS
df_all_properties["roas_ga4"] = (df_all_properties["revenue_ga4"] / df_all_properties["cost"]).fillna(0.0)

df_all_properties.replace([float('inf'), float('-inf')], 0.0, inplace=True)
df_all_properties = df_all_properties.reindex(columns=OUTPUT_COLUMNS)

# Split by date for upload
all_processed_days = {}
for date_str, group in df_all_properties.groupby("data_date"):
    all_processed_days[date_str] = group

print(f"Processed combined daywise data for {len(all_processed_days)} active days in range.")

# ========================================
# UPLOAD TO S3
# ========================================
total_cost = 0
total_clicks = 0
total_uploads = 0

for date_str, df_day in sorted(all_processed_days.items()):
    prefix = f"{S3_PREFIX}{date_str}/"
    s3_key = f"{prefix}google_ads.parquet"
    
    print(f"Uploading -> s3://{S3_BUCKET}/{s3_key}")
    
    # 1. Clear old file in the prefix
    delete_s3_prefix(S3_BUCKET, prefix)
    
    # 2. Write Parquet in memory
    buffer = io.BytesIO()
    df_day.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    # 3. Put to S3
    s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
    
    day_cost = df_day["cost"].sum()
    day_clicks = df_day["clicks"].sum()
    print(f"  Uploaded {len(df_day)} rows (Cost: ${day_cost:.2f}, Clicks: {day_clicks})")
    
    total_cost += day_cost
    total_clicks += day_clicks
    total_uploads += 1

print("\n========================================")
print("GOOGLE ADS S3 DAILY INGESTION COMPLETED")
print(f"Total uploaded files: {total_uploads}")
print(f"Total Cost processed: ${total_cost:.2f}")
print(f"Total Clicks processed: {total_clicks}")
print("========================================")


# """
# AWS Glue Job: Google Ads & GA4 Unified Daily Ingestion (Multi-Property)
# Job Type: Python Shell
# Python Version: Python 3.9+

# Job Parameters to configure in Glue:
# --additional-python-modules: PyYAML==6.0.3,pandas==3.0.3,requests==2.32.5,google-auth==2.53.0,boto3==1.43.19,pyarrow==24.0.0
# --config_bucket: ad-dl-prod-rawzone
# --gads_config_key: marketing-projects/google-ads/config/google-ads.yaml
# --ga4_config_key: marketing-projects/google-ads/config/bluesky-revenue-and-visits-2dccffa28ce3.json
# --start_date: (Optional, format: YYYY-MM-DD. Defaults to 1st of yesterday's month)
# --end_date: (Optional, format: YYYY-MM-DD. Defaults to yesterday's date)
# """

# import os
# import sys
# import io
# import json
# from datetime import datetime, timedelta
# import yaml
# import pandas as pd
# import requests
# import boto3
# from google.oauth2 import service_account
# from google.auth.transport.requests import Request

# # Initialize boto3 S3 clients
# s3_client = boto3.client("s3")
# s3_resource = boto3.resource("s3")

# # Apply custom SSL certificates for corporate network / Zscaler (useful when testing locally)
# POSSIBLE_CERTS = [
#     "/Users/vallabha.khasnivas/Desktop/work/google-analytics/combined.pem",
#     "/Users/vallabha.khasnivas/Desktop/work/google-analytics/ZscalerRootCertificate-2048-SHA256-Feb2025.crt",
#     "/Users/vallabha.khasnivas/Desktop/work/google ads/combined-certs.pem"
# ]

# ssl_cert_path = None
# for path in POSSIBLE_CERTS:
#     if os.path.exists(path):
#         ssl_cert_path = path
#         os.environ["AWS_CA_BUNDLE"] = path
#         os.environ["SSL_CERT_FILE"] = path
#         os.environ["REQUESTS_CA_BUNDLE"] = path
#         os.environ["CURL_CA_BUNDLE"] = path
#         print(f"Applying custom SSL certificate from: {path}")
#         break

# # ========================================
# # 1. RESOLVE GLUE OPTIONS
# # ========================================
# print("Resolving job arguments...")
# supported_args = [
#     "JOB_NAME",
#     "config_bucket",
#     "gads_config_key",
#     "ga4_config_key"
# ]

# optional_args = ["start_date", "end_date"]
# for opt in optional_args:
#     if f"--{opt}" in sys.argv:
#         supported_args.append(opt)

# try:
#     from awsglue.utils import getResolvedOptions
#     args = getResolvedOptions(sys.argv, supported_args)
# except ImportError:
#     print("awsglue library not found, running with local testing overrides.")
#     args = {
#         "config_bucket": "ad-dl-prod-rawzone",
#         "gads_config_key": "marketing-projects/google-ads/config/google-ads.yaml",
#         "ga4_config_key": "marketing-projects/google-ads/config/bluesky-revenue-and-visits-2dccffa28ce3.json"
#     }
#     for arg in sys.argv[1:]:
#         if arg.startswith("--"):
#             parts = arg.split("=")
#             key = parts[0][2:]
#             val = parts[1] if len(parts) > 1 else True
#             args[key] = val

# CONFIG_BUCKET = args.get("config_bucket", "ad-dl-prod-rawzone")
# GADS_CONFIG_KEY = args.get("gads_config_key", "marketing-projects/google-ads/config/google-ads.yaml")
# GA4_CONFIG_KEY = args.get("ga4_config_key", "marketing-projects/google-ads/config/bluesky-revenue-and-visits-2dccffa28ce3.json")

# # Calculate date range (defaulting to month-to-date of yesterday)
# yesterday_str = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

# user_start = args.get("start_date")
# user_end = args.get("end_date")

# if user_start and user_end:
#     START_DATE = user_start
#     END_DATE = user_end
#     print(f"Using user-specified range: {START_DATE} to {END_DATE}")
# else:
#     target_dt = datetime.strptime(yesterday_str, "%Y-%m-%d")
#     START_DATE = target_dt.replace(day=1).strftime("%Y-%m-%d")
#     END_DATE = yesterday_str
#     print(f"Defaulting to month-to-date range: {START_DATE} to {END_DATE}")

# S3_BUCKET = "ad-dl-prod-rawzone"
# S3_PREFIX = "marketing-projects/google-ads/"

# SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

# PROPERTIES = {
#     "theclubairportlounges": {
#         "ga4_property_id": "355089312",
#         "gads_customer_id": "5979083047"
#     },
#     "sleep-n-fly": {
#         "ga4_property_id": "358791306",
#         "gads_customer_id": "5471508358"
#     }
# }

# OUTPUT_COLUMNS = [
#     "property_name",
#     "property_type",
#     "data_date",
#     "campaign",
#     "ad_group",
#     "session_default_channel_group",
#     "cost",
#     "revenue",
#     "roas",
#     "clicks",
#     "impressions",
#     "ctr",
#     "avg_cpc",
#     "conversions",
#     "sessions",
#     "revenue_ga4",
#     "conversions_ga4",
#     "roas_ga4",
#     "sessions_ga4"
# ]

# # ========================================
# # 2. RETRIEVE CONFIGURATION FILES FROM S3
# # ========================================
# print(f"Downloading Google Ads config from s3://{CONFIG_BUCKET}/{GADS_CONFIG_KEY}...")
# try:
#     gads_obj = s3_client.get_object(Bucket=CONFIG_BUCKET, Key=GADS_CONFIG_KEY)
#     gads_config = yaml.safe_load(gads_obj["Body"].read().decode("utf-8"))
# except Exception as e:
#     raise Exception(f"Failed to load Google Ads config from S3: {e}")

# print(f"Downloading GA4 credentials from s3://{CONFIG_BUCKET}/{GA4_CONFIG_KEY}...")
# try:
#     ga4_obj = s3_client.get_object(Bucket=CONFIG_BUCKET, Key=GA4_CONFIG_KEY)
#     ga4_creds_dict = json.loads(ga4_obj["Body"].read().decode("utf-8"))
# except Exception as e:
#     raise Exception(f"Failed to load GA4 credentials from S3: {e}")

# # ========================================
# # 3. AUTHENTICATION
# # ========================================
# print("Authenticating with Google Analytics API...")
# credentials = service_account.Credentials.from_service_account_info(
#     ga4_creds_dict,
#     scopes=SCOPES
# )
# auth_request = Request()
# credentials.refresh(auth_request)
# GA4_ACCESS_TOKEN = credentials.token
# print("Authenticated with GA4 successfully.")

# print("Authenticating with Google Ads API (REST OAuth)...")
# token_url = "https://oauth2.googleapis.com/token"
# payload = {
#     "client_id": gads_config["client_id"],
#     "client_secret": gads_config["client_secret"],
#     "refresh_token": gads_config["refresh_token"],
#     "grant_type": "refresh_token"
# }
# response = requests.post(token_url, data=payload, verify=ssl_cert_path if ssl_cert_path else True)
# if response.status_code != 200:
#     raise Exception(f"Failed to refresh Google Ads OAuth token: {response.text}")
# gads_access_token = response.json()["access_token"]

# api_version = "v24"

# gads_headers = {
#     "Authorization": f"Bearer {gads_access_token}",
#     "developer-token": gads_config["developer_token"],
#     "Content-Type": "application/json"
# }
# if gads_config.get("login_customer_id"):
#     gads_headers["login-customer-id"] = str(gads_config["login_customer_id"])

# print("Authenticated with Google Ads successfully.")

# # ========================================
# # HELPER FUNCTIONS
# # ========================================
# def get_default_channel(campaign_name):
#     """Deterministically map campaign to PMax or Paid Search default channels."""
#     cn = campaign_name.lower()
#     if "performance max" in cn or "pmax" in cn or "performance_max" in cn:
#         return "Cross-network"
#     return "Paid Search"

# def run_google_ads_report(cust_id, query):
#     url = f"https://googleads.googleapis.com/{api_version}/customers/{cust_id}/googleAds:search"
#     results = []
#     payload = {
#         "query": query
#     }
#     while True:
#         response = requests.post(
#             url,
#             headers=gads_headers,
#             json=payload,
#             verify=ssl_cert_path if ssl_cert_path else True,
#             timeout=120
#         )
#         if response.status_code != 200:
#             raise Exception(f"Google Ads API Error (Status {response.status_code}): {response.text}")
#         res_json = response.json()
#         results.extend(res_json.get("results", []))
#         next_token = res_json.get("nextPageToken")
#         if not next_token:
#             break
#         payload["pageToken"] = next_token
#     return results

# def run_ga4_report(property_id, dimensions, metrics, start=START_DATE, end=END_DATE):
#     url = f"https://analyticsdata.googleapis.com/v1beta/properties/{property_id}:runReport"
    
#     headers = {
#         "Authorization": f"Bearer {GA4_ACCESS_TOKEN}",
#         "Content-Type": "application/json"
#     }
    
#     payload = {
#         "dateRanges": [
#             {
#                 "startDate": start,
#                 "endDate": end
#             }
#         ],
#         "dimensions": [{"name": d} for d in dimensions],
#         "metrics": [{"name": m} for m in metrics],
#         "limit": 100000
#     }
    
#     response = requests.post(
#         url,
#         headers=headers,
#         json=payload,
#         verify=ssl_cert_path if ssl_cert_path else True,
#         timeout=120
#     )
    
#     if response.status_code != 200:
#         raise Exception(f"GA4 API Error (Status {response.status_code}): {response.text}")
        
#     return response.json()

# def delete_s3_prefix(bucket_name, prefix):
#     """Deletes existing files under the specified prefix in S3."""
#     bucket = s3_resource.Bucket(bucket_name)
#     objects = list(bucket.objects.filter(Prefix=prefix))

#     if not objects:
#         return

#     print(f"  Cleaning existing objects under: s3://{bucket_name}/{prefix}")
#     for i in range(0, len(objects), 1000):
#         batch = [{"Key": obj.key} for obj in objects[i : i + 1000]]
#         bucket.delete_objects(Delete={"Objects": batch})

# # ========================================
# # MAIN LOOP OVER PROPERTIES
# # ========================================
# all_properties_merged = []

# for prop_name, prop_config in PROPERTIES.items():
#     ga4_prop_id = prop_config["ga4_property_id"]
#     gads_cust_id = prop_config["gads_customer_id"]
    
#     print(f"\n========================================")
#     print(f"Processing Property: {prop_name} (GA4: {ga4_prop_id}, Ads: {gads_cust_id})")
#     print(f"========================================")
    
#     # 1. Fetch Google Ads Native Data
#     print("1. Fetching Google Ads Native Data via REST API...")
#     query_campaign = f"""
#         SELECT
#             segments.date,
#             campaign.name,
#             metrics.cost_micros,
#             metrics.clicks,
#             metrics.impressions,
#             metrics.conversions,
#             metrics.conversions_value
#         FROM campaign
#         WHERE segments.date BETWEEN '{START_DATE}' AND '{END_DATE}'
#     """

#     query_adgroup = f"""
#         SELECT
#             segments.date,
#             campaign.name,
#             ad_group.name,
#             metrics.cost_micros,
#             metrics.clicks,
#             metrics.impressions,
#             metrics.conversions,
#             metrics.conversions_value
#         FROM ad_group
#         WHERE segments.date BETWEEN '{START_DATE}' AND '{END_DATE}'
#     """

#     print("  Querying Google Ads campaigns...")
#     gads_camp_rows = run_google_ads_report(gads_cust_id, query_campaign)
#     print(f"  Retrieved {len(gads_camp_rows)} campaign-date rows.")

#     gads_campaigns = {}
#     for row in gads_camp_rows:
#         date_str = row.get("segments", {}).get("date")
#         camp = row.get("campaign", {}).get("name", "N/A")
#         metrics = row.get("metrics", {})
#         cost = float(metrics.get("costMicros", 0)) / 1000000.0
#         clicks = int(metrics.get("clicks", 0))
#         impressions = int(metrics.get("impressions", 0))
#         conversions = float(metrics.get("conversions", 0.0))
#         revenue = float(metrics.get("conversionsValue", 0.0))
        
#         if date_str not in gads_campaigns:
#             gads_campaigns[date_str] = {}
#         gads_campaigns[date_str][camp] = {
#             "cost": cost,
#             "clicks": clicks,
#             "impressions": impressions,
#             "conversions": conversions,
#             "revenue": revenue
#         }

#     print("  Querying Google Ads ad groups...")
#     gads_adgroup_rows = run_google_ads_report(gads_cust_id, query_adgroup)
#     print(f"  Retrieved {len(gads_adgroup_rows)} adgroup-date rows.")

#     gads_adgroups = []
#     for row in gads_adgroup_rows:
#         date_str = row.get("segments", {}).get("date")
#         camp = row.get("campaign", {}).get("name", "N/A")
#         adg = row.get("adGroup", {}).get("name", "N/A")
#         metrics = row.get("metrics", {})
#         cost = float(metrics.get("costMicros", 0)) / 1000000.0
#         clicks = int(metrics.get("clicks", 0))
#         impressions = int(metrics.get("impressions", 0))
#         conversions = float(metrics.get("conversions", 0.0))
#         revenue = float(metrics.get("conversionsValue", 0.0))
        
#         gads_adgroups.append({
#             "date": date_str,
#             "campaign": camp,
#             "ad_group": adg,
#             "cost": cost,
#             "clicks": clicks,
#             "impressions": impressions,
#             "conversions": conversions,
#             "revenue": revenue
#         })

#     df_gads_adgroup = pd.DataFrame(gads_adgroups)

#     # Process Google Ads daily hybrid merge
#     gads_records = []
#     for date_str, campaigns_on_date in gads_campaigns.items():
#         df_gads_adgroup_on_date = df_gads_adgroup[df_gads_adgroup["date"] == date_str] if not df_gads_adgroup.empty else pd.DataFrame()
        
#         for campaign, camp_metrics in campaigns_on_date.items():
#             df_camp_adgroups = df_gads_adgroup_on_date[df_gads_adgroup_on_date["campaign"] == campaign] if not df_gads_adgroup_on_date.empty else pd.DataFrame()
            
#             if not df_camp_adgroups.empty:
#                 for _, row in df_camp_adgroups.iterrows():
#                     gads_records.append({
#                         "data_date": date_str,
#                         "campaign": row["campaign"],
#                         "ad_group": row["ad_group"],
#                         "cost": row["cost"],
#                         "clicks": row["clicks"],
#                         "impressions": row["impressions"],
#                         "conversions": row["conversions"],
#                         "revenue": row["revenue"]
#                     })
#             else:
#                 gads_records.append({
#                     "data_date": date_str,
#                     "campaign": campaign,
#                     "ad_group": "(campaign level)",
#                     "cost": camp_metrics["cost"],
#                     "clicks": camp_metrics["clicks"],
#                     "impressions": camp_metrics["impressions"],
#                     "conversions": camp_metrics["conversions"],
#                     "revenue": camp_metrics["revenue"]
#                 })

#     df_gads_daily = pd.DataFrame(gads_records)
#     if df_gads_daily.empty:
#         df_gads_daily = pd.DataFrame(columns=["data_date", "campaign", "ad_group", "cost", "clicks", "impressions", "conversions", "revenue"])

#     # 2. Fetch GA4 Reconciled Data (split by dimension compatibility)
#     print("2. Fetching GA4 Reconciled Data...")
    
#     # Query monthly targets
#     print("  Querying monthly unthresholded targets (Google Ads)...")
#     monthly_gads_data = run_ga4_report(ga4_prop_id, ["googleAdsCampaignName", "googleAdsAdGroupName"], ["advertiserAdCost", "advertiserAdClicks", "advertiserAdImpressions"])
#     print("  Querying monthly unthresholded targets (GA4)...")
#     monthly_ga4_data = run_ga4_report(ga4_prop_id, ["googleAdsCampaignName", "googleAdsAdGroupName", "sessionDefaultChannelGroup"], ["purchaseRevenue", "conversions", "sessions"])

#     campaign_targets = {}
    
#     # Build from monthly GA4 report
#     for row in monthly_ga4_data.get("rows", []):
#         camp = row["dimensionValues"][0]["value"]
#         adg = row["dimensionValues"][1]["value"]
#         chan = row["dimensionValues"][2]["value"]
#         if camp == "(not set)" or not camp.strip():
#             continue
#         met_vals = row["metricValues"]
#         revenue = float(met_vals[0].get("value", 0))
#         conversions = float(met_vals[1].get("value", 0))
#         sessions = int(met_vals[2].get("value", 0))
        
#         if camp not in campaign_targets:
#             campaign_targets[camp] = {}
#         if adg not in campaign_targets[camp]:
#             campaign_targets[camp][adg] = {}
#         campaign_targets[camp][adg][chan] = {
#             "cost": 0.0, "clicks": 0, "impressions": 0,
#             "revenue": revenue, "conversions": conversions, "sessions": sessions
#         }

#     # Overlay advertiser parameters onto matching default channels
#     for row in monthly_gads_data.get("rows", []):
#         camp = row["dimensionValues"][0]["value"]
#         adg = row["dimensionValues"][1]["value"]
#         if camp == "(not set)" or not camp.strip():
#             continue
#         met_vals = row["metricValues"]
#         cost = float(met_vals[0].get("value", 0))
#         clicks = int(met_vals[1].get("value", 0))
#         impressions = int(met_vals[2].get("value", 0))
#         chan = get_default_channel(camp)
        
#         if camp not in campaign_targets:
#             campaign_targets[camp] = {}
#         if adg not in campaign_targets[camp]:
#             campaign_targets[camp][adg] = {}
#         if chan not in campaign_targets[camp][adg]:
#             campaign_targets[camp][adg][chan] = {
#                 "cost": 0.0, "clicks": 0, "impressions": 0,
#                 "revenue": 0.0, "conversions": 0.0, "sessions": 0
#             }
#         campaign_targets[camp][adg][chan]["cost"] = cost
#         campaign_targets[camp][adg][chan]["clicks"] = clicks
#         campaign_targets[camp][adg][chan]["impressions"] = impressions

#     search_campaign_targets = {}
#     for camp, adgroups in campaign_targets.items():
#         search_campaign_targets[camp] = {}
#         for adg, channels in adgroups.items():
#             if adg not in ["(not set)", ""]:
#                 search_campaign_targets[camp][adg] = channels

#     # Query daily reports
#     print("  Querying daily GA4 campaign-level report (Google Ads)...")
#     daily_camp_gads = run_ga4_report(ga4_prop_id, ["date", "googleAdsCampaignName"], ["advertiserAdCost", "advertiserAdClicks", "advertiserAdImpressions"])
#     print("  Querying daily GA4 campaign-level report (GA4)...")
#     daily_camp_ga4 = run_ga4_report(ga4_prop_id, ["date", "googleAdsCampaignName", "sessionDefaultChannelGroup"], ["purchaseRevenue", "conversions", "sessions"])
    
#     print("  Querying daily GA4 adgroup-level report (Google Ads)...")
#     daily_adgroup_gads = run_ga4_report(ga4_prop_id, ["date", "googleAdsCampaignName", "googleAdsAdGroupName"], ["advertiserAdCost", "advertiserAdClicks", "advertiserAdImpressions"])
#     print("  Querying daily GA4 adgroup-level report (GA4)...")
#     daily_adgroup_ga4 = run_ga4_report(ga4_prop_id, ["date", "googleAdsCampaignName", "googleAdsAdGroupName", "sessionDefaultChannelGroup"], ["purchaseRevenue", "conversions", "sessions"])

#     # Process daily campaign reports
#     campaigns_dict = {}
#     for row in daily_camp_ga4.get("rows", []):
#         date_raw = row["dimensionValues"][0]["value"]
#         date_str = f"{date_raw[0:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
#         camp = row["dimensionValues"][1]["value"]
#         chan = row["dimensionValues"][2]["value"]
#         met_vals = row["metricValues"]
        
#         if date_str not in campaigns_dict:
#             campaigns_dict[date_str] = {}
#         if camp not in campaigns_dict[date_str]:
#             campaigns_dict[date_str][camp] = {}
#         campaigns_dict[date_str][camp][chan] = {
#             "cost": 0.0, "clicks": 0, "impressions": 0,
#             "revenue": float(met_vals[0].get("value", 0)),
#             "conversions": float(met_vals[1].get("value", 0)),
#             "sessions": int(met_vals[2].get("value", 0))
#         }

#     for row in daily_camp_gads.get("rows", []):
#         date_raw = row["dimensionValues"][0]["value"]
#         date_str = f"{date_raw[0:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
#         camp = row["dimensionValues"][1]["value"]
#         chan = get_default_channel(camp)
#         met_vals = row["metricValues"]
        
#         if date_str not in campaigns_dict:
#             campaigns_dict[date_str] = {}
#         if camp not in campaigns_dict[date_str]:
#             campaigns_dict[date_str][camp] = {}
#         if chan not in campaigns_dict[date_str][camp]:
#             campaigns_dict[date_str][camp][chan] = {
#                 "cost": 0.0, "clicks": 0, "impressions": 0,
#                 "revenue": 0.0, "conversions": 0.0, "sessions": 0
#             }
#         campaigns_dict[date_str][camp][chan]["cost"] = float(met_vals[0].get("value", 0))
#         campaigns_dict[date_str][camp][chan]["clicks"] = int(met_vals[1].get("value", 0))
#         campaigns_dict[date_str][camp][chan]["impressions"] = int(met_vals[2].get("value", 0))

#     # Process daily adgroup reports
#     adgroup_dict = {}
#     for row in daily_adgroup_ga4.get("rows", []):
#         date_raw = row["dimensionValues"][0]["value"]
#         date_str = f"{date_raw[0:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
#         camp = row["dimensionValues"][1]["value"]
#         adg = row["dimensionValues"][2]["value"]
#         chan = row["dimensionValues"][3]["value"]
#         met_vals = row["metricValues"]
        
#         key = (date_str, camp, adg, chan)
#         adgroup_dict[key] = {
#             "cost": 0.0, "clicks": 0, "impressions": 0,
#             "revenue": float(met_vals[0].get("value", 0)),
#             "conversions": float(met_vals[1].get("value", 0)),
#             "sessions": int(met_vals[2].get("value", 0))
#         }

#     for row in daily_adgroup_gads.get("rows", []):
#         date_raw = row["dimensionValues"][0]["value"]
#         date_str = f"{date_raw[0:4]}-{date_raw[4:6]}-{date_raw[6:8]}"
#         camp = row["dimensionValues"][1]["value"]
#         adg = row["dimensionValues"][2]["value"]
#         chan = get_default_channel(camp)
#         met_vals = row["metricValues"]
        
#         key = (date_str, camp, adg, chan)
#         if key not in adgroup_dict:
#             adgroup_dict[key] = {
#                 "cost": 0.0, "clicks": 0, "impressions": 0,
#                 "revenue": 0.0, "conversions": 0.0, "sessions": 0
#             }
#         adgroup_dict[key]["cost"] = float(met_vals[0].get("value", 0))
#         adgroup_dict[key]["clicks"] = int(met_vals[1].get("value", 0))
#         adgroup_dict[key]["impressions"] = int(met_vals[2].get("value", 0))

#     adgroup_list = []
#     for (date_str, camp, adg, chan), metrics_dict in adgroup_dict.items():
#         adgroup_list.append({
#             "date": date_str, "campaign": camp, "ad_group": adg, "channel": chan,
#             **metrics_dict
#         })

#     df_adgroup = pd.DataFrame(adgroup_list)

#     daily_unthresholded_sums = {}
#     for row in adgroup_list:
#         camp = row["campaign"]
#         adg = row["ad_group"]
#         chan = row["channel"]
#         if camp not in search_campaign_targets or adg not in search_campaign_targets[camp]:
#             continue
#         if camp not in daily_unthresholded_sums:
#             daily_unthresholded_sums[camp] = {}
#         if adg not in daily_unthresholded_sums[camp]:
#             daily_unthresholded_sums[camp][adg] = {}
#         if chan not in daily_unthresholded_sums[camp][adg]:
#             daily_unthresholded_sums[camp][adg][chan] = {m: 0.0 for m in ["cost", "clicks", "impressions", "revenue", "conversions", "sessions"]}
#         for m in ["cost", "clicks", "impressions", "revenue", "conversions", "sessions"]:
#             daily_unthresholded_sums[camp][adg][chan][m] += row[m]

#     discrepancy_weights = {}
#     for camp, adgroups in search_campaign_targets.items():
#         discrepancy_weights[camp] = {}
#         for adg, channels in adgroups.items():
#             discrepancy_weights[camp][adg] = {}
#             for chan, target_metrics in channels.items():
#                 discrepancy_weights[camp][adg][chan] = {}
#                 for m in ["cost", "clicks", "impressions", "revenue", "conversions", "sessions"]:
#                     target_val = target_metrics[m]
#                     unthresholded_val = daily_unthresholded_sums.get(camp, {}).get(adg, {}).get(chan, {}).get(m, 0.0)
#                     discrepancy_weights[camp][adg][chan][m] = max(0.0, target_val - unthresholded_val)

#     carry_overs = {}
#     for camp, adgroups in search_campaign_targets.items():
#         carry_overs[camp] = {}
#         for adg, channels in adgroups.items():
#             carry_overs[camp][adg] = {}
#             for chan in channels.keys():
#                 carry_overs[camp][adg][chan] = {
#                     "clicks": 0.0,
#                     "impressions": 0.0,
#                     "sessions": 0.0
#                 }

#     def distribute_metric_with_carry(total_val, weights, carry_over_dict, is_integer=False):
#         if not weights: return {}
#         tot_weight = sum(weights.values())
#         if tot_weight <= 0:
#             proportions = {k: 1.0 / len(weights) for k in weights.keys()}
#         else:
#             proportions = {k: w / tot_weight for k, w in weights.items()}
#         if len(proportions) == 1: return {k: total_val for k in proportions.keys()}
        
#         distributed = {}
#         remaining_val = total_val
#         sorted_items = sorted(proportions.items(), key=lambda x: x[1], reverse=True)
#         for i, (adg, prop) in enumerate(sorted_items):
#             if i == len(sorted_items) - 1:
#                 distributed[adg] = remaining_val
#                 if is_integer:
#                     carry_over_dict[adg] = (total_val * prop + carry_over_dict.get(adg, 0.0)) - remaining_val
#             else:
#                 if is_integer:
#                     exact = total_val * prop + carry_over_dict.get(adg, 0.0)
#                     allocated = int(round(exact))
#                     carry_over_dict[adg] = exact - allocated
#                 else:
#                      allocated = round(total_val * prop, 6)
#                 distributed[adg] = allocated
#                 remaining_val -= allocated
#         return distributed

#     ga4_records = []
#     for date_str in sorted(campaigns_dict.keys()):
#         df_adgroup_on_date = df_adgroup[df_adgroup["date"] == date_str] if not df_adgroup.empty else pd.DataFrame()
#         for campaign, channels in campaigns_dict[date_str].items():
#             if campaign == "(not set)" or not campaign.strip(): continue
            
#             is_pmax = campaign not in search_campaign_targets or not search_campaign_targets[campaign]
            
#             for chan, camp_metrics in channels.items():
#                 if not (camp_metrics["cost"] > 0 or camp_metrics["clicks"] > 0 or camp_metrics["impressions"] > 0 or camp_metrics["revenue"] > 0 or camp_metrics["sessions"] > 0): continue
                
#                 if is_pmax:
#                     ga4_records.append({
#                         "data_date": date_str,
#                         "campaign": campaign,
#                         "ad_group": "(campaign level)",
#                         "session_default_channel_group": chan,
#                         "revenue_ga4": camp_metrics["revenue"],
#                         "conversions_ga4": camp_metrics["conversions"],
#                         "sessions_ga4": camp_metrics["sessions"]
#                     })
#                     continue
                
#                 df_camp_adgroups = df_adgroup_on_date[(df_adgroup_on_date["campaign"] == campaign) & (df_adgroup_on_date["channel"] == chan)] if not df_adgroup_on_date.empty else pd.DataFrame()
#                 day_adgroup_metrics = {}
#                 for _, row in df_camp_adgroups.iterrows():
#                     day_adgroup_metrics[row["ad_group"]] = {
#                         "revenue": row["revenue"], "conversions": row["conversions"], "sessions": row["sessions"]
#                     }
                    
#                 adgroup_total_revenue = df_camp_adgroups["revenue"].sum() if not df_camp_adgroups.empty else 0
#                 adgroup_total_conversions = df_camp_adgroups["conversions"].sum() if not df_camp_adgroups.empty else 0
#                 adgroup_total_sessions = df_camp_adgroups["sessions"].sum() if not df_camp_adgroups.empty else 0
                
#                 diff_revenue = max(0.0, camp_metrics["revenue"] - adgroup_total_revenue)
#                 diff_conversions = max(0.0, camp_metrics["conversions"] - adgroup_total_conversions)
#                 diff_sessions = max(0, camp_metrics["sessions"] - adgroup_total_sessions)
                
#                 weights_dict = discrepancy_weights.get(campaign, {})
#                 rev_weights = {adg: weights_dict[adg][chan]["revenue"] for adg in weights_dict if chan in weights_dict[adg]}
#                 conv_weights = {adg: weights_dict[adg][chan]["conversions"] for adg in weights_dict if chan in weights_dict[adg]}
#                 sess_weights = {adg: weights_dict[adg][chan]["sessions"] for adg in weights_dict if chan in weights_dict[adg]}
                
#                 carry_overs_dict = carry_overs.get(campaign, {})
#                 sess_carry = {adg: carry_overs_dict[adg][chan]["sessions"] for adg in carry_overs_dict if chan in carry_overs_dict[adg]}
                
#                 diff_rev_dist = distribute_metric_with_carry(diff_revenue, rev_weights, {}, is_integer=False)
#                 diff_conv_dist = distribute_metric_with_carry(diff_conversions, conv_weights, {}, is_integer=False)
#                 diff_sess_dist = distribute_metric_with_carry(diff_sessions, sess_weights, sess_carry, is_integer=True)
                
#                 # Update carry overs
#                 for adg, val in sess_carry.items():
#                     if campaign in carry_overs and adg in carry_overs[campaign] and chan in carry_overs[campaign][adg]:
#                         carry_overs[campaign][adg][chan]["sessions"] = val
                
#                 for adg in search_campaign_targets[campaign].keys():
#                     if chan in search_campaign_targets[campaign][adg]:
#                         orig = day_adgroup_metrics.get(adg, {"revenue": 0.0, "conversions": 0.0, "sessions": 0})
#                         ga4_records.append({
#                             "data_date": date_str,
#                             "campaign": campaign,
#                             "ad_group": adg,
#                             "session_default_channel_group": chan,
#                             "revenue_ga4": orig["revenue"] + diff_rev_dist.get(adg, 0.0),
#                             "conversions_ga4": orig["conversions"] + diff_conv_dist.get(adg, 0.0),
#                             "sessions_ga4": orig["sessions"] + diff_sess_dist.get(adg, 0)
#                         })

#     df_ga4_daily = pd.DataFrame(ga4_records)
#     if df_ga4_daily.empty:
#         df_ga4_daily = pd.DataFrame(columns=["data_date", "campaign", "ad_group", "session_default_channel_group", "revenue_ga4", "conversions_ga4", "sessions_ga4"])

#     # 3. Merge Google Ads Native + GA4 with metric splitting
#     print("3. Merging Google Ads Native with Reconciled GA4...")
    
#     # Calculate session weights to split Google Ads cost/clicks/impressions proportionally
#     ga4_sums = df_ga4_daily.groupby(["data_date", "campaign", "ad_group"])["sessions_ga4"].sum().reset_index()
#     ga4_sums.rename(columns={"sessions_ga4": "total_sessions_ga4"}, inplace=True)
    
#     df_ga4_weighted = pd.merge(df_ga4_daily, ga4_sums, on=["data_date", "campaign", "ad_group"], how="left")
#     df_ga4_weighted["session_weight"] = df_ga4_weighted["sessions_ga4"] / df_ga4_weighted["total_sessions_ga4"]
#     df_ga4_weighted["session_weight"] = df_ga4_weighted["session_weight"].fillna(1.0 / df_ga4_weighted.groupby(["data_date", "campaign", "ad_group"])["session_default_channel_group"].transform("count"))
#     df_ga4_weighted["session_weight"] = df_ga4_weighted["session_weight"].fillna(1.0)
    
#     df_merged = pd.merge(df_gads_daily, df_ga4_weighted, on=["data_date", "campaign", "ad_group"], how="left")
    
#     # Distribute native metrics by weights
#     for col in ["cost", "clicks", "impressions"]:
#         df_merged[col] = df_merged[col] * df_merged["session_weight"].fillna(1.0)
        
#     # Default missing channels (if GA4 didn't track any sessions for a Google Ads row)
#     is_pmax_series = df_merged["campaign"].str.lower().str.contains("performance max|pmax|performance_max", na=False)
#     df_merged["session_default_channel_group"] = df_merged["session_default_channel_group"].fillna(
#         pd.Series(map(lambda x: "Cross-network" if x else "Paid Search", is_pmax_series), index=df_merged.index)
#     )
    
#     if "session_weight" in df_merged.columns:
#         df_merged.drop(columns=["session_weight", "total_sessions_ga4"], inplace=True)

#     # Set NaN values to 0
#     for col in ["revenue_ga4", "conversions_ga4", "sessions_ga4"]:
#         df_merged[col] = df_merged[col].fillna(0.0)

#     # Convert GA4 revenue from USD to GBP for sleep-n-fly to match Google Ads GBP currency
#     if prop_name == "sleep-n-fly":
#         df_merged["revenue_ga4"] = df_merged["revenue_ga4"] / 1.3492

#     # Set sessions to sessions_ga4
#     df_merged["sessions"] = df_merged["sessions_ga4"]

#     # Fill standard columns
#     for col in ["cost", "clicks", "impressions", "conversions", "revenue"]:
#         df_merged[col] = df_merged[col].fillna(0.0)

#     # Add property name and type columns
#     df_merged["property_name"] = prop_name
#     df_merged["property_type"] = prop_name

#     all_properties_merged.append(df_merged)


# # ========================================
# # CONCAT AND CALCULATE DERIVED METRICS
# # ========================================
# print("\nCombining results for all properties...")
# df_all_properties = pd.concat(all_properties_merged, ignore_index=True)

# # Calculate standard derived metrics
# df_all_properties["roas"] = (df_all_properties["revenue"] / df_all_properties["cost"]).fillna(0.0)
# df_all_properties["ctr"] = (df_all_properties["clicks"] / df_all_properties["impressions"] * 100).fillna(0.0)
# df_all_properties["avg_cpc"] = (df_all_properties["cost"] / df_all_properties["clicks"]).fillna(0.0)

# # Calculate GA4 ROAS
# df_all_properties["roas_ga4"] = (df_all_properties["revenue_ga4"] / df_all_properties["cost"]).fillna(0.0)

# df_all_properties.replace([float('inf'), float('-inf')], 0.0, inplace=True)
# df_all_properties = df_all_properties.reindex(columns=OUTPUT_COLUMNS)

# # Split by date for upload
# all_processed_days = {}
# for date_str, group in df_all_properties.groupby("data_date"):
#     all_processed_days[date_str] = group

# print(f"Processed combined daywise data for {len(all_processed_days)} active days in range.")

# # ========================================
# # UPLOAD TO S3
# # ========================================
# total_cost = 0
# total_clicks = 0
# total_uploads = 0

# for date_str, df_day in sorted(all_processed_days.items()):
#     prefix = f"{S3_PREFIX}{date_str}/"
#     s3_key = f"{prefix}google_ads.parquet"
    
#     print(f"Uploading -> s3://{S3_BUCKET}/{s3_key}")
    
#     # 1. Clear old file in the prefix
#     delete_s3_prefix(S3_BUCKET, prefix)
    
#     # 2. Write Parquet in memory
#     buffer = io.BytesIO()
#     df_day.to_parquet(buffer, index=False)
#     buffer.seek(0)
    
#     # 3. Put to S3
#     s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=buffer.getvalue())
    
#     day_cost = df_day["cost"].sum()
#     day_clicks = df_day["clicks"].sum()
#     print(f"  Uploaded {len(df_day)} rows (Cost: ${day_cost:.2f}, Clicks: {day_clicks})")
    
#     total_cost += day_cost
#     total_clicks += day_clicks
#     total_uploads += 1

# print("\n========================================")
# print("GOOGLE ADS S3 DAILY INGESTION COMPLETED")
# print(f"Total uploaded files: {total_uploads}")
# print(f"Total Cost processed: ${total_cost:.2f}")
# print(f"Total Clicks processed: {total_clicks}")
# print("========================================")



