# Chainlink Analytics — Project Context

> Upload this file at the start of every Claude session to restore full project context.
> Last updated: 2026-03-25 | Version: v1.0.3

---

## What the App Does

Chainlink Analytics is a **multi-tenant B2B sales analytics platform** for beverage/CPG distributors and sales teams. It helps salesperson managers and reps track store-level product placement compliance (called "gaps"), manage reset schedules, and forecast inventory needs.

**Live app:** https://chainlinkcore-main.streamlit.app
**Stack:** Python · Streamlit 1.43 · Snowflake · OpenAI API · ReportLab · Plotly · Altair

---

## Architecture

### Entry Point
- `chainlink_core.py` — main app (was `app.py`), handles auth, tenant loading, routing
- `version.txt` — single source of version string (currently `1.0.3`)

### Auth & Tenancy
- **Auth:** `streamlit-authenticator==0.4.2` (note: `login()` no longer returns a tuple — results live in `st.session_state`)
- **Multi-tenant:** each tenant has its own Snowflake DB/schema. Tenant config (warehouse, DB, schema, encrypted private key) is loaded from `TENANTUSERDB.CHAINLINK_SCH.TOML` and `SERVICE_KEYS` at login
- **Encryption:** tenant private keys stored encrypted (Fernet) in Snowflake, decrypted at runtime using `[encryption].fernet_key` from `st.secrets`
- **Two Snowflake connections:**
  - `get_service_account_connection()` — service account, used for auth/user lookups (`TENANTUSERDB`)
  - `connect_to_tenant_snowflake(tenant_config)` — per-tenant connection, stored in `st.session_state["conn"]`
- **Roles:** `ROLE` field on user; admin flag computed via `is_admin_user()`, cached in `st.session_state["is_admin"]`
- **Account lockout:** failed login attempts tracked; account locks after threshold

### Key Session State Keys
| Key | Contents |
|---|---|
| `conn` | Active tenant Snowflake connection |
| `tenant_id` | Current tenant ID |
| `tenant_config` | Full tenant config dict (DB, schema, warehouse, etc.) |
| `user_email` | Logged-in user's email |
| `is_admin` | Boolean admin flag |
| `display_name` | User's full name (cached) |
| `cached_credentials` | Auth credentials (cached to avoid Snowflake hit on every rerun) |

---

## Navigation Structure

```
Top Nav (horizontal option_menu)
├── Home
├── Reports
│   ├── Gap Report
│   ├── Email Gap Report
│   └── Data Exports
├── Format and Upload
│   ├── Load Company Data
│   ├── Reset Schedule Processing
│   └── Distribution Grid Processing
├── AI & Forecasts  [admin only]
│   ├── Predictive Purchases
│   ├── Predictive Truck Plan
│   ├── AI-Narrative Report
│   └── Placement Intelligence
└── Admin  [admin only]
    ├── Admin Dashboard
    └── Sales Contacts Admin
```

Sidebar: logo, welcome message, logout, task indicator card, version/env badge

---

## Pages & Features

### Home (`app_pages/home.py`)
- **Execution Summary card:** total in schematic, purchased, gaps, purchased %, missed revenue (@ $40.19/gap)
- **Chain bar chart:** per-chain schematic vs purchased (Altair)
- **Salesperson summary table:** gaps + execution % per rep (from `SALESPERSON_EXECUTION_SUMMARY` view)
- **Gap History pivot:** 12 most recent snapshots, pivoted by date (from `SALESPERSON_EXECUTION_SUMMARY_TBL`)
- **"Process Gap Pivot Data" button:** calls `BUILD_GAP_TRACKING()` stored proc (once per day guard via `check_and_process_data()`)
- **Supplier Performance scatter:** filtered by sidebar multiselect

### Gap Report (`app_pages/gap_report.py`)
- Filters: Salesperson, Chain, Supplier (from Snowflake `CUSTOMERS`, `SUPPLIER_COUNTY`)
- Generates Excel gap report via `utils.reports_utils.create_gap_report()`
- UPC normalization critical: avoids `.0` float artifacts in joins (uses `normalize_upc()`)
- Snapshot logic (commented out in v1.0.3): `GAP_REPORT_RUNS` + `GAP_REPORT_SNAPSHOT` tables

### Email Gap Report (`app_pages/email_gap_report.py`)
- Most recently modified page (Mar 24) — actively being worked on
- Sends gap reports via email

### Load Company Data (`app_pages/load_company_data.py` + `load_company_sections.py`)
- 4 upload sections in expanders: Sales Report, Customers, Products, Supplier by County
- Heavily updated (Mar 24) — `load_company_data_helpers.py` is the largest file (57KB)
- Handles ingestion + formatting of raw distributor data into Snowflake

### Reset Schedule (`app_pages/reset_schedule_sections.py`)
- Step 0: Download template (XLSX) with required columns: CHAIN_NAME, STORE_NUMBER, STORE_NAME, ADDRESS, CITY, RESET_DATE, RESET_TIME
- Step 1: Upload + validate/format via `format_reset_schedule()`
- Step 2: Upload formatted file to Snowflake (delete existing chain records + insert new)
- Most recently modified file (Mar 25) — actively in development

### Distribution Grid (`app_pages/distro_grid.py` + `distro_grid_sections.py`)
- Two sections: Formatter and Uploader
- Helpers in `utils/distro_grid/` (formatters.py, schema.py)

### AI & Forecasts (admin only)
- **Predictive Purchases** (`predictive_purchases.py`)
- **Predictive Truck Plan** (`predictive_truck_plan.py`) — updated Mar 20
- **AI-Narrative Report** (`ai_narrative_report.py`) — uses OpenAI API
- **Placement Intelligence** (`ai_placement_intelligence.py`) — partially wired up, not complete

### Admin
- **Admin Dashboard** (`admin.py`) — user management, tenant settings
- **Sales Contacts Admin** (`sales_contacts_admin.py`) — updated Mar 20, manages rep contact data

---

## Snowflake Structure

### Service DB (cross-tenant)
- `TENANTUSERDB.CHAINLINK_SCH.USERDATA` — users (email, tenant_id, role, is_active, is_locked, first_name, last_name)
- `TENANTUSERDB.CHAINLINK_SCH.TOML` — tenant config (warehouse, db, schema, etc.)
- `TENANTUSERDB.CHAINLINK_SCH.SERVICE_KEYS` — encrypted tenant private keys

### Per-Tenant DB (accessed via tenant connection)
Key tables/views (names as used in queries):
- `CUSTOMERS` — store-level data (SALESPERSON, CHAIN_NAME, STORE_NUMBER, etc.)
- `SUPPLIER_COUNTY` — supplier info
- `SALESPERSON_EXECUTION_SUMMARY` — view: gaps + execution % per rep
- `SALESPERSON_EXECUTION_SUMMARY_TBL` — historical snapshots
- `GAP_REPORT_SNAPSHOT` — per-item gap detail snapshots
- `GAP_REPORT_RUNS` — snapshot run headers
- `CLIENTS` — tenant business name (`BUSINESS_NAME`)
- Stored proc: `BUILD_GAP_TRACKING()` — builds gap history pivot data

---

## File / Folder Structure

```
chainlink_core.py          ← main entry (was app.py)
version.txt                ← version string
requirements.txt
.env                       ← local dev env vars (APP_ENV etc.)

app_pages/
  home.py                  ← dashboard
  gap_report.py
  email_gap_report.py      ← active dev
  load_company_data.py
  load_company_sections.py ← active dev
  reset_schedule.py
  reset_schedule_sections.py ← active dev (Mar 25)
  distro_grid.py
  distro_grid_sections.py
  admin.py
  sales_contacts_admin.py
  predictive_purchases.py
  predictive_truck_plan.py
  ai_narrative_report.py
  ai_placement_intelligence.py  ← incomplete
  data_exports.py
  _deprecated/             ← gap_history.py moved here

nav/
  navigation_bar.py        ← top nav + submenus
  menu_styles.py
  task_indicator.py        ← task sidebar card + top banner

auth/
  login.py
  reset_password.py
  forgot_password.py

tenants/
  tenant_manager.py        ← load_tenant_config(), Fernet decrypt

sf_connector/
  service_connector.py     ← get_service_account_connection(), connect_to_tenant_snowflake()

utils/
  snowflake_utils.py       ← fetch_distinct_values(), check_and_process_data()
  auth_utils.py            ← is_admin_user(), lockout logic
  load_company_data_helpers.py  ← large (57KB), ingest logic
  reset_schedule_helpers.py     ← format_reset_schedule(), upload_reset_data()
  gap_history_helpers.py
  gap_history_mailer.py
  gap_snapshot_pipeline.py
  gap_report_builder.py
  email_utils.py
  email_gap_utils.py
  distro_grid_helpers.py
  distro_grid/             ← formatters.py, schema.py
  forecasting.py
  forecasting_truck.py
  pdf_reports.py
  sales_contacts.py
  dashboard_data/
    home_dashboard.py      ← get_execution_summary(), fetch_chain_schematic_data()
  ui_helpers.py
  home_ui_helpers.py
  org_utils.py             ← get_business_name()
  logout_utils.py
  templates/
    email_templates.py
```

---

## Dev Setup

| Item | Value |
|---|---|
| Branches | `dev` (active work), `main` (production) |
| Dev venv | `venv_dev` |
| Prod venv | `chainlink_venv` |
| Deploy | Streamlit Community Cloud (auto-deploys from `main`) |
| APP_ENV | `local` / `dev` / `production` (from `.env`) |
| Version bump | Update `version.txt` + log in `CHANGELOG.md` |

---

## Known In-Flight Work (as of v1.0.3)

- `reset_schedule_sections.py` / `reset_schedule_helpers.py` — modified Mar 25, actively being developed
- `email_gap_report.py` — modified Mar 24, actively being developed  
- `load_company_sections.py` / `load_company_data_helpers.py` — modified Mar 20-24
- `ai_placement_intelligence.py` — partially wired, AI forecasting not fully connected
- `gap_history.py` — moved to `_deprecated/`, replacement logic being built

---

## Important Dev Notes

- **Never close `st.session_state["conn"]`** inside page render functions — it's shared across pages
- **`streamlit-authenticator 0.4.2`:** `login()` returns nothing; read results from `st.session_state`
- **UPC normalization:** always run `normalize_upc()` on UPC fields before writing to Snowflake snapshots to prevent `.0` float artifacts
- **Admin guard:** always check `st.session_state["is_admin"]` server-side before rendering admin content — nav visibility alone is not sufficient
- **Snowflake connector version check:** `service_connector.py` checks connector version to set either `disable_ocsp_checks` (≥3.14.0) or `insecure_mode` (<3.14.0)
- **Missed revenue calc:** hardcoded at `$40.19/gap` in `home.py` — TODO: parameterize
