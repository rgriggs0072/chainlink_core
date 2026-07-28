# Spec: UPC Diagnostic Mode 2 — Single UPC Lookup + Shared Open Food Facts Reliability Fixes

**Target version:** v1.6.11 (proposed)
**Author:** Paul (PM)
**Date:** 2026-07-27
**Status:** Confirmed by Randy 2026-07-27 — ready for Link
**Branch:** `dev` → test on Summit (9002) → Delta Pacific (9001) → `main`

---

## 1. Problem / Context

The Admin **UPC Diagnostic Tool** (`app_pages/upc_validation_test.py`) currently does one thing: bulk-validates every product in the catalog (Mode 1 — check digit + optional Open Food Facts confirmation, run across the whole PRODUCTS table).

Two gaps identified this session:

1. **No single-UPC lookup.** If someone wants to check *one* barcode against Open Food Facts — e.g. confirming a new product before it's added to the catalog — there's no way to do that without running the full bulk validation.
2. **Rate-limit blindness.** When Open Food Facts returns a rate-limit response (HTTP 429) during the bulk loop, the current code has no way to tell that apart from "genuinely not in their database." Both look identical in the results table, which makes the "❌ Not Found in Barcode DB" count untrustworthy when Chainlink is mid-rate-limit.

## 2. Scope

**In scope:**
- New Mode 2: single UPC/barcode lookup against Open Food Facts, full product detail display.
- Shared fix: distinguish rate-limited vs. genuinely-not-found vs. network/error in the Open Food Facts call path, used by both Mode 1 and Mode 2.
- Audit + confirm Mode 1's existing inter-call delay is adequate (see §3).

**Out of scope (not this spec):**
- Chat Upload Assistant (separate queued item)
- Reset Schedule CSV/XLSX support (separate queued item)
- Provisioning tool DOCUMENTS table work (separate queued item)
- Any change to Mode 1's bulk validation logic beyond the shared API call fix

## 3. Audit Findings (current state, exact refs)

File: `app_pages/upc_validation_test.py`

- `_OFF_BASE` (line 23): `https://world.openfoodfacts.org/api/v0/product/{upc}.json` — v0 API, returns the full product JSON object (not field-filtered).
- `_fetch_off()` (lines 52–67): the only Open Food Facts call site today. Catches exceptions only — a non-1 `status` in the response body (which includes the case where OFF is rate-limiting us with a 200 + informational body, or any 4xx/5xx) is treated identically to "not found." **This is the root cause of the rate-limit/not-found ambiguity.**
- Bulk loop call site (lines 163–165): `time.sleep(0.3)` already runs after every `_fetch_off()` call. This already exceeds the 0.1–0.2s figure floated in the original queue note. Confirmed with Randy (§6.3): leave as-is.
- Nav wiring: `chainlink_core.py` lines 516–526 — UPC Diagnostic is tab `t3` inside the Admin section's `st.tabs([...])`, rendering `app_pages.upc_validation_test.render()`. Mode 2 will live inside that same page, not a new top-level nav entry.
- Existing UPC helpers already imported and reusable: `normalize_upc` (`utils/gap_history_helpers.py`), `calculate_upc_check_digit` (`utils/distro_grid/formatters.py`), local `_verify_check_digit()` (lines 31–49, handles both UPC-A 12-digit and EAN-13 13-digit).
- Known domain rule (SKILLS.md / prior session): Craft Spirits Coop uses valid EAN-13 — check-digit validation must not flag 13-digit codes as invalid UPC-A. `_verify_check_digit()` already branches on length, so this is already handled — Mode 2 must reuse this function, not reimplement.

## 4. Design

### 4.1 Shared API layer refactor

Replace the single-purpose `_fetch_off()` with a two-layer split so Mode 1 and Mode 2 share the same reliability fix:

```python
def _call_off_api(upc: str) -> dict:
    """Low-level OFF call. Never raises — returns status_code/json/error."""
    try:
        r = requests.get(_OFF_BASE.format(upc=upc), timeout=_REQUEST_TIMEOUT)
    except requests.exceptions.RequestException as exc:
        return {"status_code": None, "json": None, "error": str(exc)}
    try:
        body = r.json()
    except ValueError:
        body = None
    return {"status_code": r.status_code, "json": body, "error": None}


def _classify_off_result(raw: dict) -> str:
    """Returns one of: 'found', 'not_found', 'rate_limited', 'error'."""
    if raw["error"] is not None:
        return "error"
    if raw["status_code"] == 429:
        return "rate_limited"
    if raw["status_code"] != 200 or raw["json"] is None:
        return "error"
    if raw["json"].get("status") == 1:
        return "found"
    return "not_found"
```

`_fetch_off()` (Mode 1, slim — unchanged output shape, used in the bulk table) and a new `_fetch_off_full()` (Mode 2, full product detail) both call `_call_off_api()` + `_classify_off_result()` and branch on the result. This keeps the bulk results table exactly as-is today (no new columns forced on Mode 1) while giving Mode 2 room for a richer single-product display.

Mode 1's `BARCODE_DB_FOUND` column semantics change slightly: `True` / `False` / `"RATE_LIMITED"` / `"ERROR"` instead of today's `True` / `False` / `None`. Summary metrics (line 212, `db_not_found = results_df["BARCODE_DB_FOUND"].eq(False).sum()`) need a one-line update so rate-limited rows don't inflate the "Not Found" count.

### 4.2 Mode 2 — Single UPC Lookup

**UI placement:** nested `st.tabs()` inside `render()`, right after the tenant-connection check (after current line 117), wrapping the existing controls/results block (current lines 119–271) as "Mode 1" and adding new content as "Mode 2":

```python
mode1_tab, mode2_tab = st.tabs([
    "📋 Mode 1 — Catalog Validation",
    "🔎 Mode 2 — Single UPC Lookup",
])
with mode1_tab:
    # existing lines 119–271, unchanged except the summary-metric fix in §4.1
with mode2_tab:
    # new content below
```

**Mode 2 flow:**
1. Text input for a UPC/barcode.
2. On submit: run through `normalize_upc()` → `calculate_upc_check_digit()` → `_verify_check_digit()` (same pipeline Mode 1 already uses). If check digit fails, show a **non-blocking warning** ("check digit doesn't validate — looking it up anyway") and proceed with the lookup. This matches the existing warn-don't-block pattern from the v1.6.10 duplicate-UPC guardrail rather than hard-stopping the user.
3. Call `_fetch_off_full()`, branch on `_classify_off_result()`:
   - **found** → product card: name, brand, category, quantity, image (`st.image` if `image_front_url`/`image_url` present), Nutri-Score/Eco-Score/NOVA group *only if present in the response* — hide the field entirely rather than showing "N/A" (confirmed §6.2), since beverage/alcohol products won't always carry these.
   - **not_found** → clear message: "Not in Open Food Facts — this may be a new or private-label product, not a data error."
   - **rate_limited** → distinct message: "Open Food Facts is rate-limiting requests right now — wait a moment and try again." (This is the whole point of the fix — it must look and read differently from not_found.)
   - **error** → "Couldn't reach Open Food Facts" + the underlying error string, same pattern as today's `f"ERROR: {exc}"`.
4. No download button — single result, nothing to export. No DB write — purely ephemeral/session-scoped (confirmed §6.4).

## 5. Files touched

- `app_pages/upc_validation_test.py` — all changes above (refactor + new Mode 2 tab)
- No new files, no schema changes, no new dependencies (still just `requests`)

## 6. Decisions (confirmed by Randy, 2026-07-27)

1. **Field set for Mode 2 display** — confirmed as proposed: product name, brand, category, quantity, image, Nutri-Score/Eco-Score/NOVA shown only when present.
2. **Hide-if-absent vs. show-N/A** — confirmed: hide the field entirely when Open Food Facts doesn't return it. No "N/A" placeholders.
3. **Mode 1's existing 0.3s delay** — confirmed: leave as-is. No change to the bulk loop's inter-call timing.
4. **Mode 2 lookup logging** — confirmed: purely on-screen, ephemeral, no DB write, no audit trail.
5. **Tenant ID** — confirmed: Delta Pacific = **9001** (memory record showing 1001 is stale/incorrect — corrected here for the record). Mode 2 does not touch tenant data regardless, so this doesn't affect implementation, but the correction should propagate to any other doc still showing 1001.

## 7. Testing Plan

- Test on Summit (9002) first, per standing rule.
- Manual test matrix for Mode 2: valid UPC-A found, valid EAN-13 found (Craft Spirits Coop-style), not-found barcode, malformed input (letters, wrong length), and a forced-error case (bad network/timeout) if reasonably simulate-able.
- Confirm Mode 1's bulk run still produces identical summary counts on a known-good product set before/after the refactor (regression check on the `_classify_off_result` change).
- Link reports back using the new **Task Completion Report** format in SKILLS.md — commit hash(es), files changed, branch state, tests run.

## 8. Release Checklist (last step before merge — per SKILLS.md workflow)

- [ ] Update `CHANGELOG.md` — add to `[Unreleased]` → `New Features` (Mode 2) and `Bug Fixes` (rate-limit/not-found distinction) sections, then promote to a `[v1.6.11]` block on release.
- [ ] Bump `version.txt` to `1.6.11`.
- [ ] Both updated together, same commit, per standing rule.

---
*Signed off by Randy 2026-07-27 — ready to hand to Link as-is.*
