# Changelog — Chainlink Analytics

All notable changes to this project will be documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/).

---

## [Unreleased]
> Work in progress on `dev` branch. Move items here to the next version block on release.

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

### Backlog / Known Issues
- Add formatter validation for YES_NO column — block upload if column is empty, block if no rows have YES_NO = 1, block if any values are not 0 or 1. Silent conversion of empty to 0 during sanitization was masking bad uploads (discovered during v1.2.0 CVS rebuild)
- Add formatter validation for SKU column — warn and default to 0 if SKU column is empty or contains non-numeric values. Currently sanitization silently fills with 0 without warning
- Add formatter validation to catch SKU values with incorrect digit count — e.g. removing a digit from a valid SKU should be flagged

### Release Process Tasks
- **Keep `version.txt` in sync with `CHANGELOG.md`** — whenever `[Unreleased]` is promoted to a new version block (e.g. `[vX.X.X] — YYYY-MM-DD`), also update `E:\Development\chainlink_core\version.txt` so its single-line version string matches the new release version exactly (e.g. `v1.4.0`). Both files must be committed together as part of the release commit (`chore: release vX.X.X`)

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
- Fix AI Data Query validator falsely blocking CTE queries — `_validate_sql()` was treating CTE alias names (e.g. `cte_base`) as unknown tables; now extracts CTE names via regex before the allowed-table check and skips them
- Fix `_inject_safety_cap` missing after intermediate refactor — function was accidentally removed during the row-count rewrite; restored in follow-up commit
- Fix AI generating queries with `C.STATE` against CUSTOMERS — CUSTOMERS has no STATE column; schema prompt now explicitly notes STATE exists only in RESET_SCHEDULE
- Fix Clear button leaving `dq_input` stale in session state — Clear now also pops `dq_input` so the text field resets cleanly on rerun

### UI Changes
- Run/Clear buttons are now equal-width, side-by-side, and full-container-width with clearer labels (▶ Run Query / ✕ Clear)
- Results banner now shows exact row count with a "capped" note when the safety cap truncated results; shows full count when all rows are returned
- Example questions expander auto-collapses once a question is active
- Page subtitle updated to "Queries are read-only and tenant-scoped" (was "limited to 500 rows")

### Snowflake / DB Changes
- None

### Breaking Changes
- None

---

## [v1.2.0] — 2026-04-20

### New Features
- Placement Intelligence now compares against a filtered matched archive (DISTRO_GRID_MATCHED_ARCHIVE) instead of the raw full archive — results now reflect only true Delta Pacific placements in authorized territories, eliminating inflated/inaccurate placement counts from other distributors and out-of-territory stores
- New `archive_distro_grid()` function in `utils/distro_grid_helpers.py` — handles writing both archive tables after UPDATE_DISTRO_GRID runs, ensuring PRODUCT_ID and COUNTY are stamped before the matched archive three-way filter is applied

### Bug Fixes
- Fix Placement Intelligence showing inflated placement counts (e.g. 493 new placements at Safeway) — root cause was comparing raw unfiltered DISTRO_GRID rows against raw unfiltered archive rows; both sides now apply the three-way Delta Pacific filter for apples-to-apples comparison
- Fix matched archive always writing 0 rows — archiving was happening before UPDATE_DISTRO_GRID ran, so all rows had PRODUCT_ID = 0 and COUNTY = NULL at archive time; archiving now happens after the procedure stamps those fields
- Fix ambiguous TENANT_ID SQL compilation error in matched archive INSERT — resolved by wrapping DISTRO_GRID rows in a subquery before joining to SUPPLIER_COUNTY, eliminating column name ambiguity between the two tables
- Fix LOG table INSERT failing with invalid identifier errors — updated `insert_log_entry()` to match actual LOG table schema (EVENT_TS, LEVEL, TENANT_ID, MESSAGE, CONTEXT) replacing old mismatched column references; CONTEXT stored as VARIANT using PARSE_JSON()
- Fix PARSE_JSON() failing in VALUES clause — Snowflake does not support PARSE_JSON() in VALUES; changed to INSERT INTO ... SELECT ... FROM (SELECT 1) pattern

### UI Changes
- Upload progress steps reordered to reflect new archive-after-procedure flow: 1) Delete + Insert, 2) UPDATE_DISTRO_GRID, 3) Archive
- Improved error messaging in upload flow — three distinct error states now reported separately: upload failure, procedure failure, and archive failure, so users know exactly which step failed

### Snowflake / DB Changes
- **DISTRO_GRID_ARCHIVE renamed to DISTRO_GRID_ARCHIVE_FULL** — existing archive table renamed; purpose unchanged (full recovery backup of all chain grid rows); retention 1 year rolling
- **New table: DISTRO_GRID_MATCHED_ARCHIVE** — filtered archive containing only Delta Pacific placements passing the three-way filter (PRODUCT_ID <> 0, valid COUNTY, authorized SUPPLIER_COUNTY join); used exclusively by Placement Intelligence; retention 2 years rolling
- **DG_ARCHIVE_TRACKING updated** — added FULL_ARCHIVED_AT and MATCHED_ARCHIVED_AT timestamp columns replacing original ARCHIVED_AT; both stamped together in the same transaction at upload time
- Historical data migrated from DISTRO_GRID_ARCHIVE_FULL to DISTRO_GRID_MATCHED_ARCHIVE with three-way filter applied — DISTRO_GRID_ARCHIVE_FULL rows stamped with PRODUCT_ID and COUNTY via UPDATE logic mirroring UPDATE_DISTRO_GRID SP before migration
- DISTRO_GRID_MATCHED_ARCHIVE pruned to 2-year retention window (no rows removed as all data is within window)
- SUPPLIER_COUNTY table backed up as SUPPLIER_COUNTY_BACKUP_20260418 before migration
- Spring 2026 archives rebuilt for 6 priority chains after code deployment: Safeway (3,172), Raleys (4,700), Sprouts (397), Whole Foods (315), CVS (58), FoodMaxx (0 — pending SUPPLIER_COUNTY records for OUTLAW LIGHT BEER)
- schema.py updated — DISTRO_GRID_ARCHIVE_DB_COLUMNS renamed to DISTRO_GRID_ARCHIVE_FULL_DB_COLUMNS; new DISTRO_GRID_MATCHED_ARCHIVE_DB_COLUMNS added; DG_ARCHIVE_TRACKING_DB_COLUMNS updated with new timestamp columns

### Breaking Changes
- **DISTRO_GRID_ARCHIVE no longer exists** — renamed to DISTRO_GRID_ARCHIVE_FULL; any queries or processes referencing DISTRO_GRID_ARCHIVE must be updated to use DISTRO_GRID_ARCHIVE_FULL
- **Placement Intelligence season dropdown** must now point to DISTRO_GRID_MATCHED_ARCHIVE instead of DISTRO_GRID_ARCHIVE — callers of `fetch_distinct_values()` for the season selector must pass the new table name
- **DG_ARCHIVE_TRACKING.ARCHIVED_AT** no longer written — replaced by FULL_ARCHIVED_AT and MATCHED_ARCHIVED_AT; any queries reading ARCHIVED_AT must be updated to use FULL_ARCHIVED_AT

---

## [v1.1.2] — 2026-04-14

### New Features
- None

### Bug Fixes
- Fix Supplier by County pivot upload failing on Streamlit Cloud with `'str' object cannot be interpreted as an integer` — root cause was `TOTAL` summary column being included in the melt as a county name; now excluded via `SUMMARY_COLS` filter
- Fix Supplier by County pivot upload failing on Streamlit Cloud due to file buffer exhaustion — `format_supplier_by_county()` now accepts either a file object or a pre-read DataFrame; `load_company_sections.py` passes the already-read `raw_df` to avoid double-read on cloud
- Fix Supplier by County upload blocked by `width='stretch'` not supported in Streamlit 1.43.0 — replaced with `use_container_width=True` (reverted to compatible syntax for production)
- Fix Customers upload failing validation with `Missing required columns` — `format_customers_upload()` now includes explicit alias map for source export column names (`Chain` → `CHAIN_NAME`, `Customer Name` → `STORE_NAME`, `Salesman` → `SALESPERSON`, `Shipping Address` → `ADDRESS`, `Chain Store Number` → `STORE_NUMBER`)
- Fix Distro Grid upload leaving orphaned rows on failure — `load_data_into_distro_grid()` now wraps archive, delete, and insert steps in a single transaction; any failure rolls back all steps atomically and surfaces a clear error message
- Fix false `✅ Upload complete` message showing after a failed Distro Grid upload — added `upload_succeeded` flag so post-upload steps and success messages only fire if insert committed successfully
- Fix `UPDATE_DISTRO_GRID` SP using `GETVARIABLE('selected_chain')` session variable that was never actually set — SP now accepts `CHAIN_NAME_FILTER VARCHAR DEFAULT NULL` parameter; Python passes chain directly via `%s` bind parameter
- Fix `call_procedure_update_DG()` using fragile `SET selected_chain` session variable approach — now passes chain as a direct parameter; eliminates race condition risk when multiple users upload simultaneously

### UI Changes
- Replace all deprecated `use_container_width=True/False` with `width='stretch'`/`width='content'` across all pages after upgrading to Streamlit 1.56.0 on dev branch

### Snowflake / DB Changes
- `UPDATE_DISTRO_GRID` SP refactored: now accepts optional `CHAIN_NAME_FILTER VARCHAR DEFAULT NULL` parameter — when provided, all UPDATE statements are scoped to that chain only; when NULL (default), all chains are updated (full refresh). Enables targeted post-upload updates without touching other chains
- `UPDATE_DISTRO_GRID` SP: added Step 3 to update `YES_NO` column — set to `1` if UPC exists in `PRODUCTS` (any length variant match), `0` if not. Note: this step was subsequently removed after review; `YES_NO` is now preserved as uploaded
- Removed `YES_NO` auto-update from `UPDATE_DISTRO_GRID` SP — `YES_NO` value from the uploaded grid is preserved; SP only updates `COUNTY`, `MANUFACTURER`, and `PRODUCT_NAME`

### Breaking Changes
- `UPDATE_DISTRO_GRID` SP signature changed from no parameters to `(CHAIN_NAME_FILTER VARCHAR DEFAULT NULL)` — old no-arg call `CALL UPDATE_DISTRO_GRID()` still works (NULL default = all chains), but the old SP must be dropped before deploying the new one (`DROP PROCEDURE UPDATE_DISTRO_GRID()`)
- Streamlit upgraded from 1.43.0 to 1.56.0 on `dev` branch — `use_container_width` fully replaced with `width` parameter across all pages

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
- AI Data Query — admins can ask plain English questions about their data and get instant results. Claude generates a safe SELECT query, validates it, runs it against Snowflake, and shows results in a table with CSV download. Chain names loaded dynamically so AI always knows exact values. Includes example questions, retry logic on API overload, and friendly error messages
- Placement Intelligence fully wired end-to-end: compares current DISTRO_GRID vs archived season, shows new/removed placements by manufacturer, generates GPT-4 AI narrative summary, and supports follow-up Q&A with full manufacturer context
- New inline Reset Schedule editor for admins — edit RESET_DATE and RESET_TIME directly in table, only changed rows written back to Snowflake via targeted UPDATE statements

### Bug Fixes
- Fix Placement Intelligence AI summary reading wrong session state keys causing blank output
- Fix archive query timestamp vs date mismatch (ARCHIVED_AT vs ARCHIVE_DATE) causing zero archive rows returned
- Fix PRODUCT_ID != 0 filter wiping all current DISTRO_GRID rows where PRODUCT_ID is NULL
- Fix UPC matching — now uses same 11-digit normalization as PROCESS_GAP_REPORT (12-digit UPCs truncated to 11, matched via SQL EXISTS against PRODUCTS.CARRIER_UPC)
- Fix pd.read_sql silent empty DataFrame on shared session connection — Placement Intelligence now opens a fresh tenant connection
- Fix DEBUG st.write lines left in _format_pivot() showing on screen for all pivot uploads
- Fix RESET_TIME stripping to NULL on upload — pd.to_datetime() cannot handle time objects or AM/PM strings; replaced with robust _normalize_time() helper that handles time objects, datetime objects, AM/PM strings, 24hr strings, and Excel decimal fractions

### UI Changes
- Placement Intelligence rebuilt with persistent session state so Run Comparison and Generate AI Summary work independently without wiping each other on rerun
- Results shown in tabbed layout (New / Removed Placements) with manufacturer summary + full detail expander
- Follow-up Q&A wired with full conversation history including full manufacturer breakdown data so AI can answer specific questions accurately
- Reset Schedule page restructured with expanders (Step 1: Download & Format, Step 2: Upload, Edit — admin only)

### Snowflake / DB Changes
- Distro Grid formatter now normalizes all UPCs to full 12-digit GS1 UPC-A at upload time using check digit calculation: 11-digit → append check digit, 10-digit (Excel leading zero stripped) → pad to 11 → append check digit. Ensures clean matching against PRODUCTS.CARRIER_UPC going forward

### Breaking Changes
- None

---

## [v1.0.4] — 2026-03-25

### New Features
- None

### Bug Fixes
- Fix file uploader silently failing on Streamlit Community Cloud in Reset Schedule formatter and uploader sections — wrapped both in `st.form` to prevent rerun state wipe (matches distro grid pattern)
- Fix circular import caused by `reset_schedule_sections.py` content accidentally overwriting `utils/reset_schedule_helpers.py`

### UI Changes
- Reset Schedule uploader now validates that the selected chain dropdown matches `CHAIN_NAME` in the uploaded file — blocks upload with a clear error if mismatched

### Snowflake / DB Changes
- None

### Breaking Changes
- None

---

## [v1.0.3] — 2025-??-??
> _Fill in the release date and details from memory or git log._

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
