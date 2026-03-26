# Changelog — Chainlink Analytics

All notable changes to this project will be documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/).

---

## [Unreleased]
> Work in progress on `dev` branch. Move items here to the next version block on release.

### New Features
- Placement Intelligence fully wired end-to-end: compares current DISTRO_GRID vs archived season, shows new/removed placements by manufacturer, generates GPT-4 AI narrative summary, and supports follow-up Q&A with full manufacturer context

### Bug Fixes
- Fix Placement Intelligence AI summary reading wrong session state keys causing blank output
- Fix archive query timestamp vs date mismatch (ARCHIVED_AT vs ARCHIVE_DATE) causing zero archive rows returned
- Fix PRODUCT_ID != 0 filter wiping all current DISTRO_GRID rows where PRODUCT_ID is NULL
- Fix UPC matching — now uses same 11-digit normalization as PROCESS_GAP_REPORT (12-digit UPCs truncated to 11, matched via SQL EXISTS against PRODUCTS.CARRIER_UPC)
- Fix pd.read_sql silent empty DataFrame on shared session connection — Placement Intelligence now opens a fresh tenant connection
- Fix DEBUG st.write lines left in _format_pivot() showing on screen for all pivot uploads

### UI Changes
- Placement Intelligence rebuilt with persistent session state so Run Comparison and Generate AI Summary work independently without wiping each other on rerun
- Results shown in tabbed layout (New / Removed Placements) with manufacturer summary + full detail expander
- Follow-up Q&A wired with full conversation history including full manufacturer breakdown data so AI can answer specific questions accurately

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
