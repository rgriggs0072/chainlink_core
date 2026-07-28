# Changelog — Chainlink Analytics

All notable changes to this project will be documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/).

---

## [Unreleased]
> Work in progress on `dev` branch. Move items here to the next version block on release.

### In Progress
- None

### New Features
- None

### Bug Fixes
- None

### UI Changes
- None

### Snowflake / DB Changes
- None

### Breaking Changes
- None

---

## [v1.6.11] — 2026-07-27

### New Features
- UPC Diagnostic Tool Mode 2 — single UPC/barcode lookup against Open Food
  Facts, nested as a second tab alongside the existing bulk catalog
  validator (now "Mode 1"). Runs the barcode through the same
  normalize → check-digit pipeline Mode 1 uses (warns and proceeds on a
  bad check digit rather than blocking, matching the v1.6.10 duplicate-UPC
  pattern), then shows a product card — name, brand, category, quantity,
  image, and Nutri-Score/Eco-Score/NOVA group when OFF actually scores
  the product. Ephemeral / session-scoped only — no DB write, no audit
  trail, nothing to download.

### Bug Fixes
- Open Food Facts calls were silently 403ing on every single request —
  `requests`' default User-Agent is blocked by OFF's edge/bot protection.
  Confirmed by direct testing: identical request, only the header
  changed, 403 → 200. This means the "Check against Barcode Database"
  option has likely never successfully returned real data in production;
  every row was silently falling into the generic error path. Fixed by
  sending a descriptive `User-Agent` on every OFF call.
- Rate-limited OFF responses (HTTP 429) were indistinguishable from
  genuinely-not-found barcodes — both landed in the same bucket, making
  the "❌ Not Found in Barcode DB" count untrustworthy whenever OFF was
  throttling us. Split the OFF call into `_call_off_api()` +
  `_classify_off_result()` (found / not_found / rate_limited / error),
  shared by both Mode 1 and the new Mode 2. Mode 1's summary now shows a
  separate "🚧 Rate-Limited" count instead of folding those rows into
  "Not Found"; Mode 2 shows a distinct rate-limit message ("wait a
  moment and try again") instead of the not-found message.
- Fixed a PyArrow serialization failure introduced by the above:
  `BARCODE_DB_FOUND` mixing Python `bool`/`None`/new classification
  strings in one object column made `st.dataframe()` throw
  `ArrowInvalid` on the results table (Streamlit's automatic-fix
  fallback masked it, but it was firing on every catalog run with any
  non-found row). Column now holds one consistent type — string states
  `"FOUND"` / `"NOT_FOUND"` / `"RATE_LIMITED"` / `"ERROR"`, or `None`
  when the OFF check didn't run — Arrow-safe by construction.
- Open Food Facts returns the literal grade value `"not-applicable"` (or
  `"unknown"`) for Nutri-Score/Eco-Score on products it doesn't score
  (common for beverages/alcohol) rather than omitting the field —
  Mode 2 was showing a broken, truncated "NOT-AP…" score tile for these.
  Now treated the same as absent, per the hide-don't-show-N/A rule.

### UI Changes
- UPC Diagnostic Tool now has two tabs: "📋 Mode 1 — Catalog Validation"
  (unchanged bulk flow) and "🔎 Mode 2 — Single UPC Lookup" (new).
- Mode 1's summary row gains a "🚧 Rate-Limited" metric when the barcode
  database check is enabled.

### Snowflake / DB Changes
- None

### Breaking Changes
- `BARCODE_DB_FOUND` in Mode 1's results table/export changed from
  `True`/`False`/`None` to the strings `"FOUND"`/`"NOT_FOUND"`/
  `"RATE_LIMITED"`/`"ERROR"`, or `None` when the check didn't run. No
  other code in the repo reads this column (verified), so this only
  affects anyone parsing the downloaded Excel/CSV export by the old
  boolean values.

---

## [v1.6.10] — 2026-07-27

### New Features
- CARRIER_UPC required guardrail on product upload: blank/null UPC
  is now a hard stop at format time with row-level error detail.
  Duplicate CARRIER_UPC values trigger a non-blocking warning with
  a downloadable duplicate list for supplier verification.

### Bug Fixes
- Fix Products upload preview table rendering as index-only (no
  column data) on the duplicate-CARRIER_UPC warning path, the
  happy path, and the hard-stop path — `st.dataframe(..., width=True)`
  was silently accepted as a 1px width instead of full width.
  Switched to `width='stretch'`.

### UI Changes
- Products upload page: CARRIER_UPC now marked required in the
  column-mapping help text; duplicate-UPC warning includes a
  "Download Duplicate UPC List" button (Excel, grouped by UPC).

### Snowflake / DB Changes
- None

### Breaking Changes
- Product uploads with any blank/null/"0" CARRIER_UPC are now
  rejected outright — previously loaded with UPC as NULL. Existing
  products already in PRODUCTS with a NULL UPC are unaffected (no
  schema change); this only blocks new uploads going forward.

---

## [v1.6.9] — 2026-07-27

### New Features
- Documents & Resources library — new top-level nav section where any
  logged-in user can browse and download how-to guides as PDFs.
  Grouped by category, card-style layout, download buttons per
  document. Three launch documents seeded: How to Upload a
  Distribution Grid, How to Upload a Reset Schedule, How Chainlink
  Analytics Works.

### Bug Fixes
- None

### UI Changes
- New "Documents" item in the sidebar nav, placed after Format &
  Upload and before the admin-gated sections (AI & Forecasts, Admin).

### Snowflake / DB Changes
- New `TENANTUSERDB.CHAINLINK_SCH.DOCUMENTS` table — platform-level
  document metadata shared across all tenants (not per-tenant-database
  like every other table in the system). Nullable `TENANT_ID`: `NULL`
  means visible to all tenants, a specific tenant_id restricts to that
  tenant only. `SUMMITBEVERAGE_SRV_ROLE` and `DELTAPACIFICBEV_SRV_ROLE`
  granted USAGE on the database/schema and SELECT on the table.

### Breaking Changes
- None

---

## [v1.6.8] — 2026-07-25

### New Features
- Reset Schedule store number chain-match guardrail — same 90% match
  threshold and best-chain suggestion as Distro Grid, now shared via
  `validate_store_numbers_for_chain()` in `load_company_data_helpers.py`
  and rendered via `apply_store_number_guardrail()` in `ui_helpers.py`.
  Fires in both the formatter and uploader sections.

### Bug Fixes
- Fixed SQL injection gap in Reset Schedule's DELETE query — was
  interpolating `selected_chain` directly into the SQL string; now
  bound as a parameter (`TRIM(UPPER(CHAIN_NAME)) = %s`), matching
  Distro Grid's pattern and `SKILLS.md`'s bound-parameter rule.

### UI Changes
- Reset Schedule upload template simplified from 13 columns to 3 —
  `STORE_NUMBER | RESET_DATE | RESET_TIME`. CHAIN_NAME/STORE_NAME
  (already built) and ADDRESS/CITY/COUNTY (new) are now enriched from
  CUSTOMERS at format time instead of client-provided; STATE is
  injected as a blank placeholder (not available on CUSTOMERS).
  Formatter section (Step 1) now has a chain selector, required for
  the CUSTOMERS lookup and the guardrail.

### Snowflake / DB Changes
- Dropped `PHONE_NUMBER`, `TEAM_LEAD`, `STATUS`, `NOTES` from
  `RESET_SCHEDULE` on Summit Beverage (tenant 9002); confirmed unused
  by any downstream feature via full codebase audit. Delta Pacific
  (tenant 9001) migration pending Randy's sign-off after Summit
  validation.

### Breaking Changes
- `PHONE_NUMBER`, `TEAM_LEAD`, `STATUS`, and `NOTES` no longer exist
  on `RESET_SCHEDULE` for Summit Beverage (Delta Pacific pending).
  The admin inline editor (Section 3) no longer displays STATUS/NOTES
  — removed from its SELECT and column config to avoid an
  invalid-identifier SQL error against the migrated schema.

---

## [v1.6.7] — 2026-07-25

### New Features
- Added TENANT_ID to DG_ARCHIVE_TRACKING table and archive guard
  check for multi-tenant safety — prevents cross-tenant archive
  collisions when running multiple tenants in a shared database

### Bug Fixes
- None

### UI Changes
- None

### Snowflake / DB Changes
- `DG_ARCHIVE_TRACKING` on both Summit Beverage (tenant 9002) and
  Delta Pacific (tenant 9001) — added `TENANT_ID` column, backfilled
  existing rows with each tenant's ID, and replaced the
  `(CHAIN_NAME, SEASON)` primary key with a
  `(CHAIN_NAME, SEASON, TENANT_ID)` composite key. `distro_grid_helpers.py`
  archive guard check and archive tracking INSERT updated to match
  ahead of each tenant's migration

### Breaking Changes
- None

---

## [v1.6.6] — 2026-07-25

### New Features
- None

### Bug Fixes
- Fix Placement Intelligence querying dropped SEGMENT column —
  `get_current_and_archived_distro()` in `ai_placement_helpers.py` no
  longer selects `dg.SEGMENT` / `dga.SEGMENT` from `DISTRO_GRID` /
  `DISTRO_GRID_MATCHED_ARCHIVE`, ahead of those columns being dropped
  from the schema

### UI Changes
- None

### Snowflake / DB Changes
- Phase B distro grid simplification: dropped `SKU`, `ACTIVATION_STATUS`,
  and `SEGMENT` columns from `DISTRO_GRID`, `DISTRO_GRID_ARCHIVE_FULL`,
  and `DISTRO_GRID_MATCHED_ARCHIVE` on both Summit Beverage
  (tenant 9002) and Delta Pacific (tenant 9001). Archive INSERT
  statements in `distro_grid_helpers.py` updated to match ahead of the
  migration on each tenant; verified zero rows referencing the dropped
  columns on both after migration. Completes the simplification
  started in v1.6.5 (Phase A — Python code paths only, no schema
  changes)

### Breaking Changes
- `SKU`, `ACTIVATION_STATUS`, and `SEGMENT` no longer exist on
  `DISTRO_GRID`, `DISTRO_GRID_ARCHIVE_FULL`, or
  `DISTRO_GRID_MATCHED_ARCHIVE` for Summit Beverage or Delta Pacific.
  Any external tooling or ad hoc queries referencing these columns
  will break

---

## [v1.6.5] — 2026-07-24

### New Features
- Store number chain validation guardrail: cross-validates uploaded
  file store numbers against CUSTOMERS table before formatting runs.
  Hard stop if fewer than 90% of store numbers match the selected
  chain, with suggestion of best-matching alternative chain.

### Bug Fixes
- Fix store number chain validation guardrail failing pivot uploads
  with "No STORE_NUMBER values found" — pivot files have no
  STORE_NUMBER column; store numbers are the column headers. Added
  `detect_upload_layout()` (header-based, no data inspection) and used
  it in both the guardrail and the Distro Grid formatter UI, which now
  warns and self-corrects if the selected format dropdown doesn't
  match what the uploaded file actually is, instead of hard-failing.

### UI Changes
- Distro Grid upload templates simplified — standard template reduced
  to STORE_NUMBER, UPC, PRODUCT_NAME, YES_NO; pivot template reduced
  to UPC, Name, plus store-number columns. SKU, ACTIVATION_STATUS, and
  SEGMENT removed from all Python upload/formatting code paths
  (Snowflake table columns unchanged — schema migration deferred to
  Phase B). Pivot formatter no longer requires MANUFACTURER from the
  client file; it's omitted from the insert so Snowflake defaults it
  to NULL, then UPDATE_DISTRO_GRID backfills it post-insert, same as
  COUNTY.

### Snowflake / DB Changes
- None

### Breaking Changes
- None

---

## [v1.6.4] — 2026-06-23

### New Features
- Add **Chainlink UPC Diagnostic Tool** — new admin-only page wired as the 3rd tab (🔬 UPC Diagnostic) in the Admin section; validates every product UPC in the tenant catalog: normalizes formats via `normalize_upc()`, verifies GS1 check digits for both UPC-A (12-digit) and EAN-13 (13-digit) barcodes, and optionally checks each barcode against the Open Food Facts database; results persist in session state across reruns
- UPC Diagnostic Tool — optional **Barcode Database check** (checkbox, unchecked by default); when unchecked, only check digit validation and blank UPC detection run (faster, no internet required); when checked, full validation including barcode DB lookup runs with a progress bar
- UPC Diagnostic Tool — **summary metrics** strip shows: Total Products / ✅ Valid UPCs / 🔧 Check Digit Corrected / ⚠️ Blank/Null UPC; adds ❌ Not Found in Barcode DB metric only when the barcode check was run
- UPC Diagnostic Tool — **problem rows expander** auto-opened, label dynamically lists the count and which checks failed (check digit, barcode DB, or blank UPC); adjusts based on whether barcode DB check was run
- UPC Diagnostic Tool — **dual download buttons** with live row counts: "⬇️ Download Full Results (Excel) — all N products" (all rows) and "⬇️ Download Problems Only (CSV) — N products needing review" (problem rows only); timestamped filenames

### Bug Fixes
- Fix UPC Diagnostic Tool incorrectly flagging valid EAN-13 barcodes as bad check digit — `_verify_check_digit()` now handles both UPC-A (12-digit: odd×3 + even×1) and EAN-13 (13-digit: odd×1 + even×3) algorithms; previously all barcodes were validated as UPC-A, causing false failures on imported products (e.g. Craft Spirits Coop)
- Fix UPC Diagnostic Tool results disappearing on rerun — results now stored in `st.session_state.validation_results` so the display section survives widget interaction reruns without re-running validation

### UI Changes
- Rename all end-user-visible "Claude" references to "Chainlink AI": "What Claude Noticed" button/dialog → "What Chainlink AI Noticed" (`app_pages/home.py`); chat page header and subtitle → "Chainlink AI" and "Chainlink AI queries Snowflake directly" (`app_pages/ai_chat.py`); spinner → "Chainlink AI is thinking..." — function/variable names and API references unchanged
- Rename "OFF" / "Open Food Facts" terminology in UPC Diagnostic Tool to "Barcode Database" throughout — column headers, button labels, checkbox label, and expander text
- UPC Diagnostic Tool download buttons show dynamic product counts in labels so users know exactly what each file contains before downloading
- Remove environment label (`[L] LOCAL`, `[D] DEV`) from sidebar footer — footer now shows version + copyright only
- Update sidebar copyright from © 2025 to © 2026

### Snowflake / DB Changes
- None

### Breaking Changes
- None

---

## [v1.6.3] — 2026-06-17

### New Features
- Add `load_chain_stores(conn, tenant_id, chain_name)` to `utils/load_company_data_helpers.py` — queries CUSTOMERS for active stores of a chain and returns `{STORE_NUMBER: STORE_NAME}` dict; STORE_NUMBER normalized to string for type-safe comparison; always scoped to TENANT_ID
- Add `validate_and_enrich_chain_file(df, selected_chain, chain_store_lookup)` to `utils/load_company_data_helpers.py` — shared validation + enrichment used by both Distro Grid and Reset Schedule upload flows; checks: STORE_NUMBER column present, CHAIN_NAME matches dropdown or is auto-added, every STORE_NUMBER exists in CUSTOMERS for the selected chain, STORE_NAME auto-populated from CUSTOMERS lookup if column is absent
- Wire `load_chain_stores()` + `validate_and_enrich_chain_file()` into Distro Grid uploader (`app_pages/distro_grid_sections.py`) — runs after existing CHAIN_NAME validation, before `upload_distro_grid_to_snowflake()`; blocks upload and surfaces per-issue error messages on failure
- Wire `load_chain_stores()` + `validate_and_enrich_chain_file()` into Reset Schedule uploader (`app_pages/reset_schedule_sections.py`) — runs after existing chain mismatch check, before `upload_reset_data()`; identical error behavior
- Add `check_duplicate_store_numbers()` to `utils/load_company_data_helpers.py` — detects CHAIN_NAME + STORE_NUMBER combinations that resolve to different physical locations (different ADDRESS or CITY) in the upload file before any Snowflake write is attempted
- Add `fetch_duplicate_stores()` to `utils/ai_insights.py` — queries the live CUSTOMERS table per tenant for existing duplicate store number records (ACCOUNT_STATUS = 'ACTIVE' only); results are injected into the "What Claude Noticed" AI prompt as data integrity warnings
- "What Claude Noticed" panel now surfaces existing duplicate store number records as ⚠️ warnings with standard language: "[CHAIN] has [N] locations with store number [STORE_NUMBER] — resolve in source application before the next data upload"
- Add `.python-version` file pinned to `3.11` — belt-and-suspenders Python version pin for Streamlit Cloud

### Bug Fixes
- Fix UPC leading zeros stripped by Excel before upload — `format_uploaded_grid()` in `utils/distro_grid/formatters.py` now applies `zfill(11)` after converting float-encoded UPCs to int, preserving leading zeros before the value reaches the VARCHAR(20) Snowflake column
- Fix empty SKU values causing type errors on upload — `format_uploaded_grid()` now fills null/empty SKU with `0` and casts to int before the value reaches the NUMBER(20,0) Snowflake column
- Fix gap report and gap email crashing on Streamlit Cloud with `NotSupportedError` — replaced `fetch_pandas_all()` with `fetchall()` + `pd.DataFrame(rows, columns=cols)` in `utils/gap_report_builder.py` and `utils/email_gap_utils.py`; `fetch_pandas_all()` requires the Arrow C extension (`nanoarrow`) which is not available on Python 3.14 on Streamlit Cloud
- Fix Email Gap Report page crashing on Streamlit Cloud — restored `width='stretch'` on 6 widgets in `app_pages/email_gap_report.py` (`st.dataframe`, 2× `download_button`, 3× `button`) that were incorrectly reverted to `use_container_width=True`; Streamlit 1.56.0 removed `use_container_width` support after 2025-12-31
- Fix `validate_contacts_before_send()` crashing entire gap email send on SQL error — call is now wrapped in `try/except Exception: missing_contacts = []` in `send_gap_history_pdfs()` so a SQL failure (missing column, permissions) degrades gracefully instead of aborting all emails

### UI Changes
- Customers upload page now blocks upload with a `⛔ Upload Blocked — Duplicate Store Numbers Detected` banner and per-store expandable panels showing each conflicting address, city, and rep when duplicates are found in the upload file; upload button is suppressed until the source data is corrected
- Distro Grid and Reset Schedule upload pages now hard-stop with `⛔ Upload Blocked` banner listing specific store numbers not found in CUSTOMERS, preventing silent data quality issues downstream
- Both Distro Grid and Reset Schedule pages auto-populate CHAIN_NAME from the dropdown if the column is absent in the uploaded file, and auto-populate STORE_NAME from CUSTOMERS if the column is absent — no manual column addition required
- Disable Predictive Purchases and Predictive Truck Plan tabs with "Coming Soon" treatment via `COMING_SOON_TABS` constant — hidden until underlying data pipeline is ready; prevents client confusion over incomplete features; re-enable is a one-line change

### Snowflake / DB Changes
- New read-only query against CUSTOMERS on each Distro Grid and Reset Schedule upload: `SELECT STORE_NUMBER, STORE_NAME WHERE TENANT_ID = ? AND CHAIN_NAME = ? AND ACCOUNT_STATUS = 'ACTIVE'`; no schema changes required
- New read-only `HAVING COUNT(*) > 1` query against CUSTOMERS at session load time — grouped by CHAIN_NAME + STORE_NUMBER, scoped to TENANT_ID and ACCOUNT_STATUS = 'ACTIVE'; no schema changes required

### Breaking Changes
- None

---

## [v1.6.0] — 2026-05-19

### New Features
- Add `SALESPERSON_CHANGE_LOG` table — permanent audit log of store-level ownership transfers; written automatically on every Customers upload where salesperson assignments change
- Add `detect_ownership_changes()` to `customers_helpers.py` — before/after comparison at upload time to detect which stores changed hands; uses CHAIN_NAME + STORE_NUMBER as join key against current CUSTOMERS data
- Add `log_ownership_changes()` to `customers_helpers.py` — writes detected changes to `SALESPERSON_CHANGE_LOG` atomically with the CUSTOMERS insert (same transaction)
- Add `check_sales_contacts_coverage()` to `customers_helpers.py` — validates new reps have active SALES_CONTACTS entries at upload time; shows red blocking error in UI if any are missing
- Add `validate_contacts_before_send()` to `gap_history_mailer.py` — blocks gap report email send if any rep in the current report has no active SALES_CONTACTS entry; returns actionable error message listing missing reps
- Add ownership change notice and missing-contacts blocking error to Customers upload UI in `load_company_sections.py`
- Add Sales Contacts admin page (`app_pages/sales_contacts_admin.py`) — tenant admin UI for managing the `SALES_CONTACTS` table: add/edit/deactivate reps, set salesperson and manager email addresses, handle salesperson reassignment; used by Email Gap Report to route emails to the correct rep and manager

### Bug Fixes
- Fix gap report emails routing to departed or wrong salespeople after route reorganization — email routing now uses live `CUSTOMERS.SALESPERSON` (via `CURRENT_SALESPERSON` column added to `fetch_current_streaks()`) instead of frozen `GAP_REPORT_SNAPSHOT.SALESPERSON_NAME`
- Fix gap report emails silently skipping reps missing from SALES_CONTACTS — pre-send validation in `send_gap_history_pdfs()` now blocks send and surfaces actionable error before any emails are sent

### UI Changes
- Customers upload page now shows info notice when stores are reassigned and a blocking red error when new reps are missing from Sales Contacts
- Gap History Emailer now shows a blocking red error (no emails sent) when any rep in the report is missing from Sales Contacts
- Email gap report HTML redesigned — new card layout with header gradient, metrics row (Total Gaps / New This Week / 2–3 Weeks / 4+ Weeks with color-coded severity), two-column chains + suppliers panel, modernized execution table, and attachment note; email now uses live `CURRENT_SALESPERSON` routing so it reaches the rep currently assigned to each store

### Snowflake / DB Changes
- New table: `SALESPERSON_CHANGE_LOG` — stores TENANT_ID (VARCHAR), CHAIN_NAME, STORE_NUMBER, OLD_SALESPERSON, NEW_SALESPERSON, CHANGED_AT, UPLOAD_BATCH_ID; indexed on (TENANT_ID, CHAIN_NAME, STORE_NUMBER, CHANGED_AT) and (TENANT_ID, NEW_SALESPERSON, CHANGED_AT)
- Update `GAP_CURRENT_STREAKS` view — add join to `SALESPERSON_CHANGE_LOG` and WHERE condition to exclude pre-transfer snapshot history from new rep's streak calculation (apply in Snowflake directly; see spec Part 4)
- Remove `GAP_REPORT_SNAPSHOT` from reassignment tool table map — historical snapshot rows preserved as-is; route reorganizations handled by upload detection

### Breaking Changes
- None

### Backlog / Known Issues
- Add formatter validation for YES_NO column — block upload if column is empty, block if no rows have YES_NO = 1, block if any values are not 0 or 1. Silent conversion of empty to 0 during sanitization was masking bad uploads (discovered during v1.2.0 CVS rebuild)
- Add formatter validation for SKU column — warn and default to 0 if SKU column is empty or contains non-numeric values. Currently sanitization silently fills with 0 without warning
- Add formatter validation to catch SKU values with incorrect digit count — e.g. removing a digit from a valid SKU should be flagged

### Release Process Tasks
- **Keep `version.txt` in sync with `CHANGELOG.md`** — whenever `[Unreleased]` is promoted to a new version block, also update `E:\Development\chainlink_core\version.txt` so its single-line version string matches exactly. Both files must be committed together as part of the release commit (`chore: release vX.X.X`)

---

## [v1.5.0] — 2026-05-16

### New Features
- Add AI-drafted personalized coaching note to gap report emails — `generate_salesperson_coaching()` in `ai_insights.py` uses Claude Haiku to write 2-3 sentences of personalized coaching per salesperson based on their actual gap counts, oldest gaps, and top chains; rendered as a blue coaching card above the PDF attachment note in each email
- Add `DIAGNOSTIC` intent class to AI Data Query — when user asks "why is X not showing," AI skips SQL generation and hands off to `run_diagnostic()` automatically; returns plain-English root cause with fix instructions instead of raw data
- Add `build_narrative()` to `utils/diagnostics.py` — interprets diagnostic count results and returns ✅ / ❌ narrative (root cause + fix) instead of raw dataframe
- Add `GAP_REPORT_TMP` and `GAP_REPORT_TMP2` to AI Data Query schema context and `ALLOWED_TABLES` — AI now queries pre-built gap tables directly instead of rebuilding gap logic from raw tables
- Add `check_gap_tables_populated()` diagnostic check — fires before supplier checks; reports "run PROCESS_GAP_REPORT SP" message if gap tables are empty
- Add five gap query patterns to AI system prompt covering gaps by supplier, salesperson, county, and reset context

### Bug Fixes
- Fix Customers, Products, and Sales Report uploads failing with `NP.INT64` SQL error after pandas 2.x upgrade — explicit `int()` and `str()` casts added to all `executemany` record builders in `write_customers_to_snowflake()`, `write_products_to_snowflake()`, and `write_salesreport_to_snowflake()`
- Fix AI gap queries bypassing `SUPPLIER_COUNTY` authorization check — now uses `GAP_REPORT_TMP` exclusively for gap analysis
- Fix diagnostic firing on `LOOKUP` intent zero-row results — clean "no records found" message shown instead
- Fix `run_diagnostic()` to accept `generated_sql=None` for `DIAGNOSTIC` intent calls
- Fix cross-tenant `TENANT_ID` validator incorrectly rejecting gap table queries — `GAP_REPORT_TMP` and `GAP_REPORT_TMP2` have no `TENANT_ID` column; validator now skips check when all referenced tables are gap tables

### UI Changes
- Replace raw diagnostic dataframe display with plain-English narrative
- Add red coaching card to gap report email layout — renders above gap metrics, silently skipped if API call fails so email always sends
- Add `dq_diagnosis_counts` to session state and clear button reset block

### Snowflake / DB Changes
- Add `GAP_REPORT_TMP` and `GAP_REPORT_TMP2` table definitions to Snowflake schema skill with column definitions, mixed-case quoting warnings (`"dg_upc"`, `"sr_upc"`, `"In_Schematic"`), and no-`TENANT_ID` rule
- Add gap business rules to AI system prompt — three-layer eligibility: county authorization + chain carry + store-level placement

### Breaking Changes
- None

### Dependencies
- `ai_insights.py` new module — requires `anthropic` package (already in requirements)

---

## [v1.4.0] — 2026-05-14

### New Features
- Add AI Chat — dedicated Chat section in sidebar; Claude answers questions about data, writes and executes SQL queries, interprets results in plain English; supports full conversation history per session with clear button to reset
- Add "What Claude Noticed" modal — Home Dashboard proactively analyzes execution data on session load; surfaces up to 6 prioritized insights (warnings, observations, positives); detects week-over-week execution drops, multi-period worsening trends, high-gap chains, and standout performers
- Add `describe_table` tool to AI Chat — AI inspects exact column names and data types before writing queries, eliminating column-guessing errors

### Bug Fixes
- Fix `altair_chart()` TypeError across 11 files (`width='stretch'` → `use_container_width=True`)
- Fix SQL injection vulnerability in supplier scatter chart
- Fix login page jumping — upgraded Streamlit `1.43.0` → `1.56.0`
- Fix cross-tenant cache leak — all cached functions now use `tenant_id` as explicit cache discriminator

### UI Changes
- Color palette rebrand — updated to modern color scheme throughout the app
- Remove all "Snowflake" references from end-user-facing UI — replaced with "your Chainlink database"

### Snowflake / DB Changes
- Cache all Home dashboard Snowflake queries with 5-minute TTL and per-tenant isolation
- Cache chain names for AI Chat in session state instead of re-querying on every render

### Breaking Changes
- None

### Dependencies
- `streamlit` bumped to `1.56.0`
- `anthropic >= 0.20.0`

---

## [v1.3.1] — 2026-05-09

### New Features
- None

### Bug Fixes
- Fix AI Data Query failing on SALES_REPORT queries with `invalid identifier 'SR.COUNTY'` — COUNTY does not exist in SALES_REPORT; removed from schema context and added explicit warning to prevent Claude from referencing it
- Fix AI Data Query CTE validator incorrectly flagging CTE alias names (e.g. `SAFEWAY_STORES`) as unknown tables — validator now extracts CTE names and skips them during table validation

### UI Changes
- None

### Snowflake / DB Changes
- None

### Breaking Changes
- None

---

## [v1.3.0] — 2026-05-05

### New Features
- AI Data Query: full schema context added to AI prompt — all 6 tables (CUSTOMERS, DISTRO_GRID, PRODUCTS, SUPPLIER_COUNTY, RESET_SCHEDULE, SALES_REPORT) now include complete column lists, TENANT_ID fields, table aliases (C/DG/P/SC/RS/SR), and explicit JOIN relationships
- AI Data Query: TENANT_ID scoping enforced end-to-end — AI prompt mandates `TENANT_ID = :tenant_id` on every table; `_run_query()` injects tenant from session state as a string replacement; validates tenant_id present before executing any query
- AI Data Query: row count check before fetch — runs a `COUNT(*)` wrapper before executing the full query; shows the user exactly how many rows will be returned and warns if the 200K safety cap will be hit
- AI Data Query: user-requested LIMIT support — if the question includes "first 5", "top 10", "limit to N", the AI includes LIMIT in the generated SQL and the automatic safety cap is not injected on top of it
- AI Data Query: safety cap raised from 500 to 200,000 rows — `MAX_ROW_SAFETY_CAP = 200000`
- AI Data Query: CTE support — AI prompt now instructs Claude to use `WITH` CTEs for any multi-step or multi-table logic; validator updated to recognize CTE aliases as valid table references

### Bug Fixes
- Fix AI Data Query validator falsely blocking CTE queries — `_validate_sql()` was treating CTE alias names as unknown tables; now extracts CTE names via regex before the allowed-table check and skips them
- Fix `_inject_safety_cap` missing after intermediate refactor — restored in follow-up commit
- Fix AI generating queries with `C.STATE` against CUSTOMERS — CUSTOMERS has no STATE column; schema prompt now explicitly notes STATE exists only in RESET_SCHEDULE
- Fix Clear button leaving `dq_input` stale in session state — Clear now also pops `dq_input`

### UI Changes
- Run/Clear buttons are now equal-width, side-by-side, and full-container-width with clearer labels (▶ Run Query / ✕ Clear)
- Results banner now shows exact row count with a "capped" note when the safety cap truncated results
- Example questions expander auto-collapses once a question is active
- Page subtitle updated to "Queries are read-only and tenant-scoped" (was "limited to 500 rows")

### Snowflake / DB Changes
- None

### Breaking Changes
- None

---

## [v1.2.0] — 2026-04-20

### New Features
- Placement Intelligence now compares against a filtered matched archive (DISTRO_GRID_MATCHED_ARCHIVE) instead of the raw full archive — results now reflect only true Delta Pacific placements in authorized territories
- New `archive_distro_grid()` function in `utils/distro_grid_helpers.py` — handles writing both archive tables after UPDATE_DISTRO_GRID runs

### Bug Fixes
- Fix Placement Intelligence showing inflated placement counts (e.g. 493 new placements at Safeway) — both sides now apply the three-way Delta Pacific filter for apples-to-apples comparison
- Fix matched archive always writing 0 rows — archiving now happens after the procedure stamps PRODUCT_ID and COUNTY fields
- Fix ambiguous TENANT_ID SQL compilation error in matched archive INSERT
- Fix LOG table INSERT failing with invalid identifier errors — updated `insert_log_entry()` to match actual LOG table schema
- Fix PARSE_JSON() failing in VALUES clause — changed to INSERT INTO ... SELECT ... FROM (SELECT 1) pattern

### UI Changes
- Upload progress steps reordered to reflect new archive-after-procedure flow: 1) Delete + Insert, 2) UPDATE_DISTRO_GRID, 3) Archive
- Improved error messaging — three distinct error states reported separately: upload failure, procedure failure, archive failure

### Snowflake / DB Changes
- **DISTRO_GRID_ARCHIVE renamed to DISTRO_GRID_ARCHIVE_FULL** — retention 1 year rolling
- **New table: DISTRO_GRID_MATCHED_ARCHIVE** — filtered archive for Placement Intelligence; retention 2 years rolling
- **DG_ARCHIVE_TRACKING updated** — added FULL_ARCHIVED_AT and MATCHED_ARCHIVED_AT replacing original ARCHIVED_AT
- Historical data migrated with three-way filter applied
- Spring 2026 archives rebuilt for 6 priority chains: Safeway (3,172), Raleys (4,700), Sprouts (397), Whole Foods (315), CVS (58), FoodMaxx (0)
- schema.py updated with new column name constants

### Breaking Changes
- **DISTRO_GRID_ARCHIVE no longer exists** — renamed to DISTRO_GRID_ARCHIVE_FULL
- **Placement Intelligence season dropdown** must point to DISTRO_GRID_MATCHED_ARCHIVE
- **DG_ARCHIVE_TRACKING.ARCHIVED_AT** replaced by FULL_ARCHIVED_AT and MATCHED_ARCHIVED_AT

---

## [v1.1.2] — 2026-04-14

### New Features
- None

### Bug Fixes
- Fix file uploader silently failing on Streamlit Community Cloud in Reset Schedule sections — wrapped in `st.form`
- Fix circular import caused by `reset_schedule_sections.py` overwriting `utils/reset_schedule_helpers.py`

### UI Changes
- Reset Schedule uploader validates selected chain matches `CHAIN_NAME` in uploaded file

### Snowflake / DB Changes
- None

### Breaking Changes
- None

---

## [v1.1.1] — 2026-04-13

### New Features
- None

### Bug Fixes
- None

### UI Changes
- None

### Snowflake / DB Changes
- None

### Breaking Changes
- None

---

## [v1.1.0] — 2026-03-25

### New Features
- AI Data Query — admins ask plain English questions; Claude generates safe SELECT query, validates, runs against Snowflake, shows results with CSV download; chain names loaded dynamically; includes example questions and retry logic
- Placement Intelligence fully wired end-to-end — compares current DISTRO_GRID vs archived season, shows new/removed placements by manufacturer, generates AI narrative summary, supports follow-up Q&A
- New inline Reset Schedule editor — edit RESET_DATE and RESET_TIME directly in table; only changed rows written back via targeted UPDATE statements

### Bug Fixes
- Fix Placement Intelligence AI summary reading wrong session state keys
- Fix archive query timestamp vs date mismatch causing zero archive rows
- Fix PRODUCT_ID != 0 filter wiping all DISTRO_GRID rows where PRODUCT_ID is NULL
- Fix UPC matching — now uses same 11-digit normalization as PROCESS_GAP_REPORT
- Fix pd.read_sql silent empty DataFrame on shared session connection
- Fix DEBUG st.write lines showing on screen for all pivot uploads
- Fix RESET_TIME stripping to NULL on upload — replaced with robust `_normalize_time()` helper

### UI Changes
- Placement Intelligence rebuilt with persistent session state
- Results shown in tabbed layout (New / Removed Placements)
- Reset Schedule page restructured with expanders

### Snowflake / DB Changes
- Distro Grid formatter normalizes all UPCs to full 12-digit GS1 UPC-A at upload time using check digit calculation

### Breaking Changes
- None

---

## [v1.0.4] — 2026-03-25

### New Features
- None

### Bug Fixes
- Fix file uploader silently failing on Streamlit Community Cloud in Reset Schedule sections — wrapped in `st.form`
- Fix circular import caused by `reset_schedule_sections.py` overwriting `utils/reset_schedule_helpers.py`

### UI Changes
- Reset Schedule uploader validates selected chain matches `CHAIN_NAME` in uploaded file

### Snowflake / DB Changes
- None

### Breaking Changes
- None

---

## [v1.0.3] — 2025-??-??
> _Fill in release date and details from git log._

### New Features
-

### Bug Fixes
-

### UI Changes
-

### Snowflake / DB Changes
-

### Breaking Changes
- None

---

## [v1.0.2] — 2025-??-??

### New Features
-

### Bug Fixes
-

### UI Changes
-

### Snowflake / DB Changes
-

### Breaking Changes
- None

---

## [v1.0.1] — 2025-??-??

### New Features
-

### Bug Fixes
-

### UI Changes
-

### Snowflake / DB Changes
-

### Breaking Changes
- None

---

## [v1.0.0] — 2025-??-??
> Initial release of Chainlink Analytics.

### New Features
- Initial app launch on Streamlit Community Cloud
- Snowflake backend integration

### Bug Fixes
- None (initial release)

### UI Changes
- None (initial release)

### Snowflake / DB Changes
- Initial schema setup

### Breaking Changes
- None

---

<!--
## RELEASE TEMPLATE — copy this block for each new version

## [vX.X.X] — YYYY-MM-DD

### New Features
-

### Bug Fixes
-

### UI Changes
-

### Snowflake / DB Changes
-

### Breaking Changes
-

-->
