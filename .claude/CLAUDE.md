# Chainlink Core — Project Instructions

## What This App Does

**Chainlink Core** is a multi-tenant B2B sales analytics platform for beverage distributors and their sales teams. It is a **Streamlit** web app backed by **Snowflake**.

The core workflow runs twice a year (spring and fall):

1. **Grocery chains publish reset schedules (RS)** — dates when store shelves are reset/planogrammed.
2. **Chains publish distribution grids (DG)** — which products are authorized to be placed on the shelf.
3. Users upload RS and DG files into the app; the app formats and stores them in Snowflake.
4. **Salespeople use the app** to see when each store's reset is scheduled and which products they are authorized to place.
5. Users upload a **"did buy" buy report** from EncompassTech (the distributor's sales software) to see if stores actually purchased the authorized products.
6. Any authorized product that was **not purchased** is called a **gap**. The app surfaces and reports gaps so reps can follow up.

---

## Terminology (use these terms consistently)

| Abbreviation | Full term |
|---|---|
| **DG** | Distribution Grid — list of products authorized for shelf placement per chain |
| **RS** | Reset Schedule — calendar of store shelf-reset dates per chain |
| **gap** | An authorized product that a store did not purchase |
| **EncompassTech** | Third-party software that produces the "did buy" sales report |

---

## Tech Stack

- **Frontend:** Python · Streamlit (`dev` branch: **1.56.0** · `main` branch: **1.43.0** — verify before targeting an API)
- **Backend / Data Store:** Snowflake (multi-tenant; each tenant has its own DB/schema)
- **AI features:** OpenAI API
- **Charting:** Plotly · Altair
- **PDF generation:** ReportLab
- **Auth:** streamlit-authenticator 0.4.2
- **Hosting:** Streamlit Community Cloud (`chainlinkcore-main.streamlit.app`, auto-deploys from `main`)

---

## Key Files

| File | Purpose |
|---|---|
| `chainlink_core.py` | Main entry point — auth, tenant loading, routing |
| `version.txt` | Single source of truth for the app version string (format: `vX.X.X`) |
| `CHANGELOG.md` | Release changelog — updated every version bump |
| `requirements.txt` | Production dependencies |
| `.env` | Local dev env vars (`APP_ENV=local`) |

---

## Directory Structure

```
app_pages/          ← one file per page/feature
auth/               ← login, password reset, forgot password
nav/                ← navigation bar, menu styles, task indicator
sf_connector/       ← Snowflake connection helpers
tenants/            ← tenant config loading + key decryption
utils/              ← business logic helpers (gap, reports, email, forecast, etc.)
  distro_grid/      ← DG formatters and schema helpers
  dashboard_data/   ← home dashboard data fetchers
  templates/        ← email templates
import_templates/   ← downloadable Excel templates for users
images/             ← logos and static images
```

---

## Snowflake Architecture

### Service DB (cross-tenant)
- `TENANTUSERDB.CHAINLINK_SCH.USERDATA` — users
- `TENANTUSERDB.CHAINLINK_SCH.TOML` — tenant config (warehouse, db, schema)
- `TENANTUSERDB.CHAINLINK_SCH.SERVICE_KEYS` — encrypted tenant private keys

### Per-Tenant DB (accessed after login)
Key tables and views:
- `CUSTOMERS` — store-level records (salesperson, chain, store number)
- `SUPPLIER_COUNTY` — supplier info
- `SALESPERSON_EXECUTION_SUMMARY` — view: gaps + execution % per rep
- `SALESPERSON_EXECUTION_SUMMARY_TBL` — historical snapshots
- `CLIENTS` — tenant business name
- Stored proc: `BUILD_GAP_TRACKING()` — builds gap history pivot

### Two Snowflake Connections
- `get_service_account_connection()` — service account, used only for auth/user lookups
- `connect_to_tenant_snowflake(tenant_config)` — per-tenant, stored in `st.session_state["conn"]`

---

## Key Session State Keys

| Key | Contents |
|---|---|
| `conn` | Active tenant Snowflake connection |
| `tenant_id` | Current tenant ID |
| `tenant_config` | Full tenant config dict |
| `user_email` | Logged-in user's email |
| `is_admin` | Boolean admin flag |
| `display_name` | User's full name |

---

## Development Rules

### After every completed and tested feature or fix:
1. **Bump the version** in `version.txt` (format: `vMAJOR.MINOR.PATCH`)
2. **Add a changelog entry** in `CHANGELOG.md` describing what changed and why
3. Use the `changelog` skill for the full release workflow

### For any SQL touching CUSTOMERS, DISTRO_GRID, PRODUCTS, SUPPLIER_COUNTY, RESET_SCHEDULE, or SALES_REPORT:
- Invoke the `snowflake-schema` skill before writing the query — it has the full table/column/JOIN reference

---

## Streamlit Coding Rules

### `width='stretch'` replaces `use_container_width` (Streamlit 1.56.0+)

`use_container_width` is deprecated and will be removed after 2025-12-31. Always use `width` on `dev`.

| Deprecated | Use this |
|---|---|
| `use_container_width=True` | `width='stretch'` |
| `use_container_width=False` | `width='content'` |

Applies to: `st.button`, `st.dataframe`, `st.image`, `st.plotly_chart`, `st.altair_chart`, `st.download_button`, `st.form_submit_button`, `st.columns`, and any other component that accepts a width argument.

**Exception:** Files targeting the `main` branch (Streamlit 1.43.0) must still use `use_container_width` — `width` is not supported there. Default assumption is `dev` unless told otherwise.

### Wrap file uploaders in `st.form`

Always wrap file uploaders in `st.form` to prevent Streamlit Community Cloud from wiping uploader state on rerun. Root cause of the v1.0.4 Reset Schedule upload bug.

```python
with st.form("uploader_form"):
    file = st.file_uploader("Upload file", type=["xlsx"])
    submitted = st.form_submit_button("Upload", width='stretch')
    if submitted and file:
        # process file
```

### Read file buffers once — pass a DataFrame, not the file object

When a file uploader's bytes are needed in multiple places, read once into a DataFrame and pass the DataFrame. Streamlit Community Cloud exhausts file buffers on a second read — root cause of the v1.1.2 Supplier by County bug.

---

## Snowflake / Data Rules

### TENANT_ID scoping is mandatory

Every query that touches tenant data MUST include `TENANT_ID = :tenant_id` (or equivalent bind). Pull `tenant_id` from session state — never hardcode, never trust user input.

### Cast numpy types to native Python before Snowflake writes

When building records for `cursor.executemany()`, explicitly cast DataFrame values to native Python types. Pandas 2.x returns `np.int64`, `np.float64`, `np.bool_` more aggressively, and the Snowflake connector raises `NP.INT64` SQL compilation errors on them.

```python
# WRONG — numpy types leak through
records = [(row["STORE_NUMBER"], row["TENANT_ID"]) for _, row in df.iterrows()]

# RIGHT — explicit casts
records = [
    (
        int(row["STORE_NUMBER"]) if pd.notna(row["STORE_NUMBER"]) else None,
        int(row["TENANT_ID"]),
    )
    for _, row in df.iterrows()
]
```

**Cheat sheet:**
- Integer → `int(value) if pd.notna(value) else None`
- Float → `float(value) if pd.notna(value) else None`
- String → `str(value) if pd.notna(value) else None`
- Boolean → `bool(value)`
- Date/datetime → `value.to_pydatetime() if pd.notna(value) else None`
- NaN/NaT/None → always map to `None` for SQL `NULL`

Apply to all `write_*_to_snowflake()` functions — add the casts from the start on any new ones.

### UPC normalization is mandatory

Always run `normalize_upc()` on UPC fields before writing to Snowflake to prevent `.0` float artifacts in joins.

---

## Security / Auth Rules

- **Never close `st.session_state["conn"]`** inside page render functions — it is shared across all pages
- **Always check `st.session_state["is_admin"]` server-side** before rendering admin content — nav visibility alone is not sufficient
- Admin-only features are guarded by `is_admin_user()` from `utils/auth_utils.py`
- **`streamlit-authenticator 0.4.2`:** `login()` returns nothing — read auth results from `st.session_state`

---

## Known Pitfalls (bugs we have fixed before — do not repeat)

- **Do not use `pd.to_datetime()` on time-only values** — use the `_normalize_time()` helper in `utils/`. Root cause of v1.1.0 RESET_TIME bug.
- **Do not call `PARSE_JSON()` inside a SQL `VALUES` clause** — Snowflake does not support it. Use `INSERT INTO ... SELECT ... FROM (SELECT 1)` pattern instead (v1.2.0 LOG table fix).
- **Do not archive DISTRO_GRID rows BEFORE running `UPDATE_DISTRO_GRID`** — PRODUCT_ID and COUNTY won't be stamped yet, and the matched archive will write 0 rows. Archive AFTER the procedure (v1.2.0 fix).
- **Do not reference `DISTRO_GRID_ARCHIVE`** — it was renamed to `DISTRO_GRID_ARCHIVE_FULL` in v1.2.0. Placement Intelligence reads from `DISTRO_GRID_MATCHED_ARCHIVE`.
- **Missed revenue is hardcoded at `$40.19/gap`** in `home.py` — do not change without direction.

---

## Deployment

| Item | Value |
|---|---|
| Dev branch | `dev` |
| Production branch | `main` |
| Dev venv | `venv_dev` |
| Prod venv | `chainlink_venv` |
| `APP_ENV` | `local` / `dev` / `production` (set in `.env`) |
