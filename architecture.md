# Architecture Notes

## Data Flow

```
Graph API → Databricks (PySpark) → Delta Lake → ADF → Power Automate → Email
```

---

## Delta Lake Schema

### Bronze: `app_registrations_snapshot`

| Column | Type | Description |
|---|---|---|
| app_id | string | Azure Object ID of app registration |
| app_name | string | Display name |
| client_id | string | Application (client) ID |
| credential_id | string | Key ID of secret/certificate |
| credential_type | string | Secret or Certificate |
| display_name | string | Friendly name of credential |
| start_date | date | Credential start date |
| end_date | date | Credential expiry date |
| owner_names | string | Pipe-separated owner display names |
| owner_emails | string | Pipe-separated owner email addresses |
| snapshot_date | timestamp | When this snapshot was taken |

### Silver: `credential_expiry_status`

Adds computed columns:

| Column | Type | Description |
|---|---|---|
| days_to_expiry | int | Days until expiry (negative = expired) |
| expiry_status | string | HEALTHY / WARNING / CRITICAL / EXPIRED |
| is_replaced | boolean | True if a newer credential exists |
| alert_required | boolean | True if alert should be sent |

---

## ADF Pipeline Structure

```
PL_EDWH_AppRegExpiryAlert (Master)
├── ACT_NB_FetchCredentials        → Runs notebook 01
├── ACT_NB_ProcessExpiryLogic      → Runs notebook 02
├── ACT_NB_GenerateAlertPayload    → Runs notebook 03
└── ACT_HTTP_TriggerPowerAutomate  → POST to Power Automate HTTP trigger
```

---

## Key Design Decisions

1. **Graph Batch API** over sequential calls — 5x faster for owner fetching
2. **Window functions** for replacement detection — no manual cross-joins
3. **Pipe-separated multi-owner storage** — simple, query-friendly
4. **Medallion architecture** — clean separation of raw vs processed data
5. **Power Automate for email** — zero additional infra cost
