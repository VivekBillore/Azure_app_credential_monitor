# Notebook 3: Generate Alert Payload for Power Automate
# Demo version — no sensitive data
# ============================================================
# Reads Silver layer, groups credentials by owner email,
# builds JSON payload, writes to Gold layer, and triggers
# Power Automate HTTP flow per owner
# ============================================================

import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_list, struct, to_json

spark = SparkSession.builder.appName("GenerateAlertPayload").getOrCreate()

SILVER_PATH       = "/mnt/demo/silver/credential_expiry_status"
GOLD_PATH         = "/mnt/demo/gold/alert_dispatch_log"
POWER_AUTOMATE_URL = dbutils.secrets.get(scope="app-cred-monitor", key="power-automate-url")

# ============================================================
# STEP 1: Load Silver layer — only credentials needing alerts
# ============================================================
df = spark.read.format("delta").load(SILVER_PATH)
alerts_df = df.filter(col("alert_required") == True)

print(f"Credentials requiring alerts: {alerts_df.count()}")

# ============================================================
# STEP 2: Explode pipe-separated owners to individual rows
# Each owner receives only their own apps' credentials
# ============================================================
from pyspark.sql.functions import split, explode, trim, arrays_zip, posexplode_outer

# Split pipe-separated owners into arrays
alerts_df = alerts_df \
    .withColumn("owner_email_arr", split(col("owner_emails"), "\\|")) \
    .withColumn("owner_name_arr",  split(col("owner_names"),  "\\|"))

# Explode: one row per owner
alerts_df = alerts_df \
    .withColumn("owner_zip", arrays_zip("owner_name_arr", "owner_email_arr")) \
    .withColumn("owner",     explode("owner_zip")) \
    .withColumn("owner_name",  col("owner.owner_name_arr")) \
    .withColumn("owner_email", trim(col("owner.owner_email_arr"))) \
    .filter(col("owner_email") != "")

# ============================================================
# STEP 3: Group credentials by owner → one payload per person
# ============================================================
payload_df = alerts_df.groupBy("owner_name", "owner_email").agg(
    collect_list(
        struct(
            "app_name", "credential_type", "display_name",
            "end_date", "days_to_expiry", "expiry_status"
        )
    ).alias("credentials")
)

payloads = payload_df.collect()
print(f"Unique owners to alert: {len(payloads)}")

# ============================================================
# STEP 4: Send HTTP trigger to Power Automate per owner
# ============================================================
dispatch_log = []

for row in payloads:
    payload = {
        "owner_name":  row["owner_name"],
        "owner_email": row["owner_email"],
        "credentials": [
            {
                "app_name":        c["app_name"],
                "credential_type": c["credential_type"],
                "display_name":    c["display_name"],
                "end_date":        str(c["end_date"]),
                "days_to_expiry":  c["days_to_expiry"],
                "expiry_status":   c["expiry_status"]
            }
            for c in row["credentials"]
        ]
    }

    try:
        resp = requests.post(
            POWER_AUTOMATE_URL,
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=30
        )
        status = "Success" if resp.status_code in (200, 202) else "Failed"
    except Exception as e:
        status = f"Error: {str(e)}"

    dispatch_log.append({
        "owner_email":   row["owner_email"],
        "cred_count":    len(row["credentials"]),
        "dispatch_status": status
    })
    print(f"  {row['owner_email']}: {status} ({len(row['credentials'])} credentials)")

# ============================================================
# STEP 5: Write dispatch log to Gold layer
# ============================================================
log_df = spark.createDataFrame(dispatch_log)
log_df.write.format("delta").mode("append").save(GOLD_PATH)

print(f"\n✅ Alert dispatch complete. Log written to: {GOLD_PATH}")
log_df.show()
