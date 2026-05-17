# Notebook 2: Process Expiry Logic + Replacement Detection
# Demo version — no sensitive data
# ============================================================
# Reads Bronze snapshot, computes days-to-expiry, classifies
# status, detects credential replacements via Window Functions,
# flags alerts, writes to Silver Delta layer
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, datediff, current_date, when, lag, lead,
    row_number, lit, to_date
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("AppRegExpiryLogic").getOrCreate()

BRONZE_PATH = "/mnt/demo/bronze/app_registrations_snapshot"
SILVER_PATH = "/mnt/demo/silver/credential_expiry_status"

# --------------------------------------------------------
# STEP 1: Load Bronze snapshot
# --------------------------------------------------------
df = spark.read.format("delta").load(BRONZE_PATH)
df = df.withColumn("end_date", to_date(col("end_date")))

# --------------------------------------------------------
# STEP 2: Calculate days to expiry
# --------------------------------------------------------
df = df.withColumn("days_to_expiry", datediff(col("end_date"), current_date()))

# --------------------------------------------------------
# STEP 3: Classify expiry status
# --------------------------------------------------------
df = df.withColumn("expiry_status",
    when(col("days_to_expiry") < 0,   lit("EXPIRED"))
    .when(col("days_to_expiry") <= 5,  lit("CRITICAL"))
    .when(col("days_to_expiry") <= 15, lit("WARNING"))
    .otherwise(lit("HEALTHY"))
)

# --------------------------------------------------------
# STEP 4: Replacement Detection using Window Functions
# If a new secret was added for the same app around the
# same time an old one expired → it's a replacement
# No alert needed for replaced credentials
# --------------------------------------------------------
window_spec = Window.partitionBy("app_id", "credential_type").orderBy("end_date")

df = df.withColumn("prev_end_date", lag("end_date", 1).over(window_spec))
df = df.withColumn("next_end_date", lead("end_date", 1).over(window_spec))

df = df.withColumn("is_replaced",
    when(
        (col("expiry_status").isin("EXPIRED", "CRITICAL")) &
        (col("next_end_date").isNotNull()) &
        (datediff(col("next_end_date"), col("end_date")) <= 30),
        lit(True)
    ).otherwise(lit(False))
)

# --------------------------------------------------------
# STEP 5: Flag credentials that need alerts
# --------------------------------------------------------
df = df.withColumn("alert_required",
    when(
        (col("expiry_status").isin("EXPIRED", "CRITICAL", "WARNING")) &
        (col("is_replaced") == False),
        lit(True)
    ).otherwise(lit(False))
)

# --------------------------------------------------------
# STEP 6: Write to Silver layer
# --------------------------------------------------------
df.write.format("delta").mode("overwrite").save(SILVER_PATH)

# Summary
print("=== Expiry Summary ===")
df.groupBy("expiry_status").count().orderBy("expiry_status").show()
print(f"Credentials needing alerts: {df.filter(col('alert_required') == True).count()}")
