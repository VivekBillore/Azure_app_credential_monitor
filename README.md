# Azure App Registration Credential Expiry Monitor

An enterprise-grade automated monitoring system that tracks **Azure App Registration secrets and certificates** across a tenant, detects upcoming expirations, and sends personalized multi-tier email alerts to credential owners — eliminating manual monitoring effort by **90%**.

---

## Problem Statement

In large organizations, Azure App Registrations (Service Principals) are used by hundreds of pipelines and applications. When a secret or certificate expires without warning:
- **Pipelines fail silently** in production
- **Manual tracking** via spreadsheets is error-prone and time-consuming
- **No visibility** into who owns which credential

This project solves all three problems with a fully automated, near zero-cost daily monitoring pipeline.

---

## Architecture

```
Azure AD Tenant
        |
        ▼
Microsoft Graph API ─────────────────────┐
        |
        ▼
Databricks Notebook (PySpark)
├── Fetch all App Registrations
├── Fetch secrets & certificates per app
├── Fetch owners via Graph Batch API
├── Detect replacements (Window Functions)
└── Write snapshots to Delta Lake
        |
        ▼
Azure Data Factory (ADF)
├── PL_EDWH_AppRegExpiryAlert (Master Pipeline)
│       ├── ACT_NB_FetchCredentials (Databricks Notebook)
│       ├── ACT_NB_ProcessExpiryLogic
│       └── ACT_HTTP_TriggerPowerAutomate
└── Scheduled: Daily 6AM UTC
        |
        ▼
Power Automate
└── Send personalized HTML email alerts to owners
        |
        ▼
Delta Lake (ADLS)
├── bronze/app_registrations_snapshot
├── silver/credential_expiry_status
└── gold/alert_dispatch_log
```

---

## Alert Logic

| Days to Expiry | Alert Type | Frequency | Action Required |
|---|---|---|---|
| 15 days | Warning Email | Once | Plan renewal |
| 1–5 days | Urgent Daily Alert | Daily | Renew immediately |
| 0 (expired) | Expired Notification | Monthly digest | Renew / Investigate |
| Replaced | Suppressed | Never | No action needed |

---

## Tech Stack

| Component | Technology |
|---|---|
| Credential Fetching | Microsoft Graph API (Batch) |
| Data Processing | PySpark, Databricks (Unity Catalog) |
| Storage | Delta Lake (Bronze/Silver/Gold), ADLS Gen2 |
| Orchestration | Azure Data Factory |
| Alerting | Power Automate, HTML Email |
| Secret Management | Databricks Secret Scope |

---

## Key Technical Highlights

- **Graph Batch API** — fetches owners for 100 apps in a single HTTP request instead of 100 sequential calls
- **PySpark Window Functions** — detects credential replacements to suppress redundant alerts
- **Recursive team traversal** — walks org hierarchy to find all team members under a manager
- **Medallion Architecture** — Bronze (raw) → Silver (processed) → Gold (alerts) in Delta Lake
- **Pipe-separated storage** — owner name/email stored as `Name1|Name2` for multi-owner support

---

## Project Structure

```
azure-app-credential-monitor/
├── notebooks/
│   ├── 01_fetch_app_registrations.py   # Graph API fetch + Delta write
│   ├── 02_process_expiry_logic.py      # Window functions + alert flagging
│   └── 03_generate_alert_payload.py    # Build JSON payload for Power Automate
├── adf_pipeline/
│   └── PL_AppRegExpiryAlert_demo.json  # ADF pipeline export (sanitized)
├── data_samples/
│   └── sample_credential_data.csv      # Dummy data for demo
├── email_templates/
│   └── alert_email_template.html       # HTML email template
├── docs/
│   └── architecture.md                 # Detailed design notes
└── README.md
```

---

## Impact

- Monitors **5,000+ Service Principal credentials** across enterprise tenant
- Reduced manual monitoring effort by **90%**
- **Zero pipeline failures** due to expired credentials since deployment
- Near **zero infrastructure cost** — runs on existing Databricks + ADF

---

## Setup (Demo)

```bash
# 1. Clone the repo
git clone https://github.com/VivekBillore/azure-app-credential-monitor

# 2. Install dependencies
pip install pyspark delta-spark msal requests

# 3. Configure credentials (use environment variables - never hardcode)
export TENANT_ID="your-tenant-id"
export CLIENT_ID="your-client-id"
export CLIENT_SECRET="your-client-secret"

# 4. Run notebooks in order
# 01 → 02 → 03
```

> ⚠️ **Note:** This is a sanitized demo version. All company-specific data, endpoints, and secrets have been removed.
