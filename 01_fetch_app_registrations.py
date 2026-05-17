# Notebook 1: Fetch App Registrations from Microsoft Graph API
# Demo version — no sensitive data
# ============================================================
# Fetches all App Registrations + secrets/certificates + owners
# Writes raw snapshot to Delta Lake Bronze layer
# ============================================================

import requests
import json
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType
)

spark = SparkSession.builder.appName("FetchAppRegistrations").getOrCreate()

# ============================================================
# CONFIG — use Databricks Secret Scope in production
# ============================================================
TENANT_ID     = dbutils.secrets.get(scope="app-cred-monitor", key="tenant-id")
CLIENT_ID     = dbutils.secrets.get(scope="app-cred-monitor", key="client-id")
CLIENT_SECRET = dbutils.secrets.get(scope="app-cred-monitor", key="client-secret")

BRONZE_PATH   = "/mnt/demo/bronze/app_registrations_snapshot"
GRAPH_BASE    = "https://graph.microsoft.com/v1.0"

# ============================================================
# STEP 1: Get OAuth2 token
# ============================================================
def get_access_token(tenant_id, client_id, client_secret):
    url  = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    data = {
        "grant_type":    "client_credentials",
        "client_id":     client_id,
        "client_secret": client_secret,
        "scope":         "https://graph.microsoft.com/.default"
    }
    resp = requests.post(url, data=data)
    resp.raise_for_status()
    return resp.json()["access_token"]

token   = get_access_token(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

# ============================================================
# STEP 2: Fetch all App Registrations (paginated)
# ============================================================
def fetch_all_pages(url, headers):
    results = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data  = resp.json()
        results.extend(data.get("value", []))
        url   = data.get("@odata.nextLink")
    return results

print("Fetching app registrations...")
apps = fetch_all_pages(f"{GRAPH_BASE}/applications?$select=id,appId,displayName", headers)
print(f"  Found {len(apps)} app registrations")

# ============================================================
# STEP 3: Fetch owners via Graph Batch API (100 apps per batch)
# ============================================================
def fetch_owners_batch(app_ids, headers):
    """Single HTTP request fetches owners for up to 100 apps — 5x faster."""
    requests_payload = [
        {"id": str(i), "method": "GET", "url": f"/applications/{app_id}/owners?$select=displayName,mail"}
        for i, app_id in enumerate(app_ids)
    ]
    resp = requests.post(
        f"{GRAPH_BASE}/$batch",
        headers=headers,
        json={"requests": requests_payload}
    )
    resp.raise_for_status()
    responses = resp.json().get("responses", [])
    owner_map = {}
    for r in responses:
        app_id = app_ids[int(r["id"])]
        owners = r.get("body", {}).get("value", [])
        names  = "|".join(o.get("displayName", "") for o in owners)
        emails = "|".join(o.get("mail", "") or "" for o in owners)
        owner_map[app_id] = {"names": names, "emails": emails}
    return owner_map

BATCH_SIZE = 100
owner_map  = {}
for i in range(0, len(apps), BATCH_SIZE):
    batch    = [a["id"] for a in apps[i:i+BATCH_SIZE]]
    owner_map.update(fetch_owners_batch(batch, headers))
    print(f"  Owner batch {i//BATCH_SIZE + 1} fetched")

# ============================================================
# STEP 4: Build flattened credential rows
# ============================================================
snapshot_ts = datetime.now(timezone.utc)
rows        = []

for app in apps:
    app_id   = app["id"]
    app_name = app.get("displayName", "")
    client_id_val = app.get("appId", "")
    owners   = owner_map.get(app_id, {"names": "", "emails": ""})

    # Secrets
    for cred in app.get("passwordCredentials", []):
        rows.append({
            "app_id":          app_id,
            "app_name":        app_name,
            "client_id":       client_id_val,
            "credential_id":   cred.get("keyId", ""),
            "credential_type": "Secret",
            "display_name":    cred.get("displayName", ""),
            "start_date":      str(cred.get("startDateTime", ""))[:10],
            "end_date":        str(cred.get("endDateTime", ""))[:10],
            "owner_names":     owners["names"],
            "owner_emails":    owners["emails"],
            "snapshot_date":   str(snapshot_ts)
        })

    # Certificates
    for cred in app.get("keyCredentials", []):
        rows.append({
            "app_id":          app_id,
            "app_name":        app_name,
            "client_id":       client_id_val,
            "credential_id":   cred.get("keyId", ""),
            "credential_type": "Certificate",
            "display_name":    cred.get("displayName", ""),
            "start_date":      str(cred.get("startDateTime", ""))[:10],
            "end_date":        str(cred.get("endDateTime", ""))[:10],
            "owner_names":     owners["names"],
            "owner_emails":    owners["emails"],
            "snapshot_date":   str(snapshot_ts)
        })

print(f"\nTotal credential rows: {len(rows)}")

# ============================================================
# STEP 5: Write to Delta Lake Bronze layer
# ============================================================
df = spark.createDataFrame(rows)
df.write.format("delta").mode("overwrite").save(BRONZE_PATH)

print(f"✅ Bronze snapshot written to: {BRONZE_PATH}")
df.groupBy("credential_type").count().show()
