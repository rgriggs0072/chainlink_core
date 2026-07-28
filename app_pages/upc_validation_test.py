# app_pages/upc_validation_test.py
"""
Chainlink UPC Diagnostic Tool — Admin Page

Pulls all products from PRODUCTS, normalizes each CARRIER_UPC, validates the
GS1 check digit, then hits Open Food Facts to confirm the barcode is live.

Permanent admin feature for validating PRODUCTS UPC data quality.
"""

import time
from datetime import datetime
from io import BytesIO

import pandas as pd
import requests
import streamlit as st

from utils.gap_history_helpers import normalize_upc
from utils.distro_grid.formatters import calculate_upc_check_digit


_OFF_BASE = "https://world.openfoodfacts.org/api/v0/product/{upc}.json"
_REQUEST_TIMEOUT = 6  # seconds
# OFF's edge (nginx/bot protection) 403s requests' default User-Agent outright —
# confirmed by direct testing during the v1.6.11 reliability audit: identical
# request, only the header changed, 403 -> 200. Without this, every OFF call
# in both modes silently classifies as "error" no matter what the barcode is.
_OFF_HEADERS = {
    "User-Agent": "ChainlinkAnalytics-UPCDiagnostic/1.6.11 (contact: randy@chainlinkanalytics.com)"
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _verify_check_digit(upc_str: str) -> bool:
    """Validate check digit — handles UPC-A (12-digit) and EAN-13 (13-digit)."""
    if not upc_str.isdigit():
        return False
    if len(upc_str) == 12:
        digits   = [int(d) for d in upc_str]
        odd_sum  = sum(digits[i] for i in range(0, 11, 2))
        even_sum = sum(digits[i] for i in range(1, 11, 2))
        total    = (odd_sum * 3) + even_sum
        expected = (10 - (total % 10)) % 10
        return digits[11] == expected
    if len(upc_str) == 13:
        digits   = [int(d) for d in upc_str]
        odd_sum  = sum(digits[i] for i in range(0, 12, 2))
        even_sum = sum(digits[i] for i in range(1, 12, 2))
        total    = odd_sum + (even_sum * 3)
        expected = (10 - (total % 10)) % 10
        return digits[12] == expected
    return False


def _call_off_api(upc: str) -> dict:
    """Low-level OFF call. Never raises — returns status_code/json/error."""
    try:
        r = requests.get(_OFF_BASE.format(upc=upc), headers=_OFF_HEADERS, timeout=_REQUEST_TIMEOUT)
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


def _fetch_off(upc: str) -> dict:
    """Slim OFF result for the Mode 1 bulk table.

    off_found is one of "FOUND" / "NOT_FOUND" / "RATE_LIMITED" / "ERROR"
    (string, not bool) — the column also holds None for rows where the OFF
    check didn't run at all. A True/False/None mix broke PyArrow's type
    inference the moment a "RATE_LIMITED"/"ERROR" string landed in the same
    object column (it infers bool from the first values, then chokes on the
    first string), so every state is a string for Arrow-safe serialization
    in st.dataframe(). Keep it string-or-None; don't reintroduce bools here.
    """
    raw = _call_off_api(upc)
    outcome = _classify_off_result(raw)

    if outcome == "found":
        p = raw["json"].get("product", {})
        return {
            "off_found": "FOUND",
            "off_product_name": p.get("product_name", ""),
            "off_brand": p.get("brands", ""),
            "off_barcode": p.get("code", upc),
        }
    if outcome == "not_found":
        return {"off_found": "NOT_FOUND", "off_product_name": "", "off_brand": "", "off_barcode": ""}
    if outcome == "rate_limited":
        return {"off_found": "RATE_LIMITED", "off_product_name": "", "off_brand": "", "off_barcode": ""}
    return {
        "off_found": "ERROR",
        "off_product_name": f"ERROR: {raw['error']}" if raw["error"] else "ERROR: unexpected response",
        "off_brand": "",
        "off_barcode": "",
    }


def _fetch_off_full(upc: str) -> dict:
    """Full OFF result for Mode 2's single-UPC lookup — includes the raw
    product payload (for the detail card) plus the same classification Mode 1
    uses, so both modes agree on what 'rate-limited' vs. 'not found' means."""
    raw = _call_off_api(upc)
    outcome = _classify_off_result(raw)
    product = raw["json"].get("product", {}) if outcome == "found" else {}
    return {
        "outcome": outcome,
        "product": product,
        "error": raw["error"],
        "status_code": raw["status_code"],
    }


def _load_products(conn, tenant_id: int) -> pd.DataFrame:
    sql = """
        SELECT PRODUCT_ID, SUPPLIER, PRODUCT_NAME, PACKAGE, CARRIER_UPC
        FROM PRODUCTS
        WHERE TENANT_ID = %s
        ORDER BY SUPPLIER, PRODUCT_NAME
    """
    with conn.cursor() as cur:
        cur.execute(sql, (int(tenant_id),))
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    return pd.DataFrame(rows, columns=cols)


def _to_excel(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="UPC Validation")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

def render() -> None:
    if not st.session_state.get("is_admin"):
        st.warning("Admin access required.")
        st.stop()

    conn      = st.session_state.get("conn")
    tenant_id = st.session_state.get("tenant_id")

    if "validation_results" not in st.session_state:
        st.session_state.validation_results = None
    if "validation_check_off" not in st.session_state:
        st.session_state.validation_check_off = False
    if "mode2_lookup_result" not in st.session_state:
        st.session_state.mode2_lookup_result = None

    st.title("🔬 Chainlink UPC Diagnostic Tool")
    st.caption(
        "Chainlink AI validates every product UPC in your catalog — normalizing formats, "
        "verifying GS1 check digits, and confirming each barcode against the Open Food Facts "
        "database to catch bad data before it causes silent gap report failures."
    )

    if not conn or not tenant_id:
        st.error("No active tenant connection.")
        return

    mode1_tab, mode2_tab = st.tabs([
        "📋 Mode 1 — Catalog Validation",
        "🔎 Mode 2 — Single UPC Lookup",
    ])

    # ==================================================================
    # Mode 1 — Catalog Validation (bulk)
    # ==================================================================
    with mode1_tab:
        # --------------------------------------------------------------
        # Controls
        # --------------------------------------------------------------
        check_off = st.checkbox(
            "Check against Barcode Database (requires internet — slower)",
            value=False,
        )

        btn_col, clear_col = st.columns([4, 1])
        with btn_col:
            run_clicked = st.button("▶ Run Validation", type="primary", width="stretch")
        with clear_col:
            if st.button("🔄 Clear", width="stretch"):
                st.session_state.validation_results = None
                st.rerun()

        # --------------------------------------------------------------
        # Validation (only runs when button is clicked)
        # --------------------------------------------------------------
        if run_clicked:
            st.session_state.validation_check_off = check_off

            with st.spinner("Loading products from Snowflake…"):
                df = _load_products(conn, tenant_id)

            if df.empty:
                st.warning("No products found for this tenant.")
                st.session_state.validation_results = None
            else:
                if check_off:
                    est_minutes = len(df) * 4.5 / 60
                    st.info(
                        f"Found **{len(df):,}** products. Querying barcode database — "
                        f"this will take approximately {est_minutes:.0f} minutes."
                    )
                else:
                    st.info(f"Found **{len(df):,}** products. Running check digit validation…")

                results = []
                progress = st.progress(0, text="Starting…")
                total = len(df)

                for i, row in enumerate(df.itertuples(index=False), start=1):
                    raw_upc    = str(row.CARRIER_UPC) if row.CARRIER_UPC is not None else ""
                    normalized = normalize_upc(raw_upc) or ""
                    upc_12     = calculate_upc_check_digit(normalized) if normalized else ""
                    check_ok   = _verify_check_digit(upc_12)

                    if check_off and upc_12:
                        off = _fetch_off(upc_12)
                        # OFF's documented limit is 15 req/min per IP (4.0s min
                        # spacing) for read product queries — 4.5s gives margin.
                        # Found via Delta Pacific testing: 0.3s (~200 req/min)
                        # was 13x over the limit and only surfaced at real scale.
                        time.sleep(4.5)
                    else:
                        off = {"off_found": None, "off_product_name": "", "off_brand": "", "off_barcode": ""}

                    results.append({
                        "PRODUCT_ID":       row.PRODUCT_ID,
                        "SUPPLIER":         row.SUPPLIER,
                        "PRODUCT_NAME":     row.PRODUCT_NAME,
                        "PACKAGE":          row.PACKAGE,
                        "RAW_UPC":          raw_upc,
                        "NORMALIZED_UPC":   normalized,
                        "UPC_12":           upc_12,
                        "CHECK_DIGIT_OK":   check_ok,
                        "BARCODE_DB_FOUND": off["off_found"],
                        "BARCODE_DB_NAME":  off["off_product_name"],
                        "BARCODE_DB_BRAND": off["off_brand"],
                        "BARCODE_DB_CODE":  off["off_barcode"],
                    })

                    progress.progress(i / total, text=f"{i}/{total} — {row.PRODUCT_NAME[:40]}")

                progress.empty()
                st.session_state.validation_results = pd.DataFrame(results)

        # --------------------------------------------------------------
        # Display (reads from session state — survives reruns)
        # --------------------------------------------------------------
        if st.session_state.validation_results is not None:
            results_df   = st.session_state.validation_results
            ran_with_off = st.session_state.get("validation_check_off", False)

            # 1. Summary stats
            total_products  = len(results_df)
            blank_upc_count = results_df["NORMALIZED_UPC"].eq("").sum()
            valid_upc_count = results_df["CHECK_DIGIT_OK"].eq(True).sum()
            corrected_count = (
                results_df["CHECK_DIGIT_OK"].eq(True)
                & (results_df["UPC_12"] != results_df["RAW_UPC"])
                & results_df["NORMALIZED_UPC"].ne("")
            ).sum()

            st.markdown("---")
            st.subheader("Summary")

            if ran_with_off:
                # Rate-limited/error rows carry their own BARCODE_DB_FOUND
                # values ("RATE_LIMITED"/"ERROR", see _classify_off_result)
                # rather than being folded into "NOT_FOUND", so they no
                # longer inflate this count the way they silently did before
                # the OFF call was classified.
                db_not_found    = results_df["BARCODE_DB_FOUND"].eq("NOT_FOUND").sum()
                db_rate_limited = results_df["BARCODE_DB_FOUND"].eq("RATE_LIMITED").sum()
                c1, c2, c3, c4, c5, c6 = st.columns(6)
                c1.metric("Total Products",             total_products)
                c2.metric("✅ Valid UPCs",              int(valid_upc_count - corrected_count))
                c3.metric("🔧 Check Digit Corrected",   int(corrected_count))
                c4.metric("❌ Not Found in Barcode DB", int(db_not_found))
                c5.metric("🚧 Rate-Limited",             int(db_rate_limited))
                c6.metric("⚠️ Blank / Null UPC",       int(blank_upc_count))
            else:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Total Products",           total_products)
                c2.metric("✅ Valid UPCs",            int(valid_upc_count - corrected_count))
                c3.metric("🔧 Check Digit Corrected", int(corrected_count))
                c4.metric("⚠️ Blank / Null UPC",     int(blank_upc_count))

            # 2. Problem rows
            st.markdown("---")
            st.subheader("Results")

            if ran_with_off:
                is_problem = (
                    ~results_df["CHECK_DIGIT_OK"]
                    | ~results_df["BARCODE_DB_FOUND"].eq("FOUND")
                    | results_df["NORMALIZED_UPC"].eq("")
                )
                problem_label = "bad check digit, not in barcode database, or blank UPC"
            else:
                is_problem = (
                    ~results_df["CHECK_DIGIT_OK"]
                    | results_df["NORMALIZED_UPC"].eq("")
                )
                problem_label = "bad check digit or blank UPC"

            problem_df = results_df[is_problem]
            if not problem_df.empty:
                with st.expander(
                    f"⚠️ {len(problem_df)} problem rows ({problem_label})",
                    expanded=True,
                ):
                    st.dataframe(problem_df, width="stretch", hide_index=True)

            # 3. Full table
            st.dataframe(results_df, width="stretch", hide_index=True)

            # 4. Downloads
            dl1, dl2 = st.columns(2)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            dl1.download_button(
                label=f"⬇️ Download Full Results (Excel) — all {total_products:,} products",
                data=_to_excel(results_df),
                file_name=f"upc_validation_{ts}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                width="stretch",
            )
            dl2.download_button(
                label=f"⬇️ Download Problems Only (CSV) — {len(problem_df):,} products needing review",
                data=problem_df.to_csv(index=False).encode("utf-8"),
                file_name=f"upc_problems_{ts}.csv",
                mime="text/csv",
                width="stretch",
            )

    # ==================================================================
    # Mode 2 — Single UPC Lookup
    # ==================================================================
    with mode2_tab:
        st.caption(
            "Look up one barcode against Open Food Facts — useful for confirming a "
            "new product before it's added to the catalog. Nothing here is saved; "
            "it's a one-off lookup, not part of the catalog validation run."
        )

        with st.form("mode2_lookup_form"):
            raw_input = st.text_input(
                "UPC / Barcode",
                placeholder="e.g. 041000001375",
                key="mode2_upc_input",
            )
            submitted = st.form_submit_button("🔎 Look Up", type="primary")

        if submitted:
            normalized = normalize_upc(raw_input) or ""
            if not normalized:
                st.session_state.mode2_lookup_result = None
                st.warning("Enter a UPC/barcode with at least one digit.")
            else:
                upc_12   = calculate_upc_check_digit(normalized)
                check_ok = _verify_check_digit(upc_12)

                with st.spinner(f"Looking up {upc_12} on Open Food Facts…"):
                    off = _fetch_off_full(upc_12)

                st.session_state.mode2_lookup_result = {
                    "upc_12":      upc_12,
                    "check_ok":    check_ok,
                    "outcome":     off["outcome"],
                    "product":     off["product"],
                    "error":       off["error"],
                    "status_code": off["status_code"],
                }

        result = st.session_state.mode2_lookup_result
        if result:
            if not result["check_ok"]:
                st.warning(
                    f"⚠️ Check digit doesn't validate for **{result['upc_12']}** — "
                    "looking it up anyway."
                )

            outcome = result["outcome"]

            if outcome == "found":
                product = result["product"]
                st.success(f"✅ Found on Open Food Facts — {result['upc_12']}")

                img_col, info_col = st.columns([1, 3])
                image_url = product.get("image_front_url") or product.get("image_url")
                if image_url:
                    img_col.image(image_url, width="stretch")

                with info_col:
                    name = product.get("product_name")
                    if name:
                        st.markdown(f"**{name}**")
                    brand = product.get("brands")
                    if brand:
                        st.write(f"Brand: {brand}")
                    category = product.get("categories")
                    if category:
                        st.write(f"Category: {category}")
                    quantity = product.get("quantity")
                    if quantity:
                        st.write(f"Quantity: {quantity}")

                    # OFF returns "unknown"/"not-applicable" as the literal grade
                    # value for products it doesn't score (common for beverages/
                    # alcohol) rather than omitting the field — treat those the
                    # same as absent per the hide-don't-show-N/A decision.
                    _UNSCORED = {"", "unknown", "not-applicable"}
                    nutriscore = product.get("nutriscore_grade")
                    ecoscore   = product.get("ecoscore_grade")
                    nova       = product.get("nova_group")
                    show_nutri = bool(nutriscore) and str(nutriscore).lower() not in _UNSCORED
                    show_eco   = bool(ecoscore) and str(ecoscore).lower() not in _UNSCORED
                    show_nova  = nova is not None and str(nova).lower() not in _UNSCORED
                    if show_nutri or show_eco or show_nova:
                        score_cols = st.columns(3)
                        if show_nutri:
                            score_cols[0].metric("Nutri-Score", str(nutriscore).upper())
                        if show_eco:
                            score_cols[1].metric("Eco-Score", str(ecoscore).upper())
                        if show_nova:
                            score_cols[2].metric("NOVA Group", str(nova))

            elif outcome == "not_found":
                st.info(
                    f"Not in Open Food Facts ({result['upc_12']}) — this may be a new "
                    "or private-label product, not a data error."
                )
            elif outcome == "rate_limited":
                st.warning(
                    "Open Food Facts is rate-limiting requests right now — "
                    "wait a moment and try again."
                )
            else:  # error
                detail = result["error"] or f"unexpected response (status {result['status_code']})"
                st.error(f"Couldn't reach Open Food Facts — {detail}")
