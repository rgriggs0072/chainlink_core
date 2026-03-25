# Changelog — Chainlink Analytics

All notable changes to this project will be documented in this file.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versioning follows [Semantic Versioning](https://semver.org/).

---

## [Unreleased]
> Work in progress on `dev` branch. Move items here to the next version block on release.

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
