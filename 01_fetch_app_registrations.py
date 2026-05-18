# Notebook 1: Fetch App Registrations from Microsoft Graph API
# Demo version — no sensitive data

import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from delta.tables import DeltaTable
import os

spark = SparkSession.builder.appName("AppRegCredentialMonitor").getOrCreate()

# --------------------------------------------------------
# CONFIG — use environment variables or Databricks secrets
# --------------------------------------------------------
TENANT_ID     = os.getenv("TENANT_ID",     "demo-tenant-id")
CLIENT_ID     = os.getenv("CLIENT_ID",     "demo-client-id")
CLIENT_SECRET = os.getenv("CLIENT_SECRET", "demo-client-secret")
DELTA_PATH    = "/mnt/demo/bronze/app_registrations_snapshot"

# --------------------------------------------------------
# STEP 1: Get Access Token
# --------------------------------------------------------
def get_access_token(tenant_id, client_id, client_secret):
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    payload = {
        "grant_type":    "client_credentials",
        "client_id":     client_id,
        "client_secret": client_secret,
        "scope":         "https://graph.microsoft.com/.default"
    }
    response = requests.post(url, data=payload)
    response.raise_for_status()
    return response.json()["access_token"]

# --------------------------------------------------------
# STEP 2: Fetch ALL App Registrations (paginated)
# --------------------------------------------------------
def fetch_all_app_registrations(token):
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://graph.microsoft.com/v1.0/applications?$select=id,appId,displayName,passwordCredentials,keyCredentials"
    apps = []
    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        apps.extend(data.get("value", []))
        url = data.get("@odata.nextLink")  # Handle pagination
    return apps

# --------------------------------------------------------
# STEP 3: Fetch Owners via Graph Batch API
# (100 apps per batch request — efficient)
# --------------------------------------------------------
def fetch_owners_batch(token, app_ids):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type":  "application/json"
    }
    batch_url = "https://graph.microsoft.com/v1.0/$batch"
    results   = {}

    # Process in chunks of 20 (Graph batch Limit)
    for i in range(0, len(app_ids), 20):
        chunk = app_ids[i:i+20]
        requests_payload = {
            "requests": [
                {
                    "id":     str(idx),
                    "method": "GET",
                    "url":    f"/applications/{app_id}/owners"
                }
                for idx, app_id in enumerate(chunk)
            ]
        }
        response = requests.post(batch_url, headers=headers, json=requests_payload)
        response.raise_for_status()

        for resp in response.json().get("responses", []):
            app_id = chunk[int(resp["id"])]
            owners = resp.get("body", {}).get("value", [])
            # Store as pipe-separated string: "Name1|Name2"
            owner_names  = "|".join([o.get("displayName", "")                            for o in owners])
            owner_emails = "|".join([o.get("mail", "") or o.get("userPrincipalName", "") for o in owners])
            results[app_id] = {"owner_names": owner_names, "owner_emails": owner_emails}

    return results

# --------------------------------------------------------
# STEP 4: Flatten credentials into rows
# --------------------------------------------------------
def flatten_credentials(apps, owners_map):
    rows = []
    for app in apps:
        app_id     = app["id"]
        app_name   = app["displayName"]
        client_id  = app["appId"]
        owner_info = owners_map.get(app_id, {"owner_names": "", "owner_emails": ""})

        # Secrets
        for secret in app.get("passwordCredentials", []):
            rows.append({
                "app_id":          app_id,
                "app_name":        app_name,
                "client_id":       client_id,
                "credential_id":   secret.get("keyId"),
                "credential_type": "Secret",
                "display_name":    secret.get("displayName", ""),
                "start_date":      str(secret.get("startDateTime", "")),
                "end_date":        str(secret.get("endDateTime", "")),
                "owner_names":     owner_info["owner_names"],
                "owner_emails":    owner_info["owner_emails"],
            })

        # Certificates
        for cert in app.get("keyCredentials", []):
            rows.append({
                "app_id":          app_id,
                "app_name":        app_name,
                "client_id":       client_id,
                "credential_id":   cert.get("keyId"),
                "credential_type": "Certificate",
                "display_name":    cert.get("displayName", ""),
                "start_date":      str(cert.get("startDateTime", "")),
                "end_date":        str(cert.get("endDateTime", "")),
                "owner_names":     owner_info["owner_names"],
                "owner_emails":    owner_info["owner_emails"],
            })
    return rows

# --------------------------------------------------------
# MAIN
# --------------------------------------------------------
token   = get_access_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
apps    = fetch_all_app_registrations(token)
app_ids = [a["id"] for a in apps]
owners  = fetch_owners_batch(token, app_ids)
rows    = flatten_credentials(apps, owners)

# Write to Delta Lake (Bronze Layer)
df = spark.createDataFrame(rows)
df = df.withColumn("snapshot_date", current_timestamp())

df.write.format("delta").mode("overwrite").save(DELTA_PATH)
print(f"■ Snapshot written: {df.count()} credential records")
