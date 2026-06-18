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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _verify_check_digit(upc12: str) -> bool:
    if len(upc12) != 12 or not upc12.isdigit():
        return False
    odd_sum  = sum(int(upc12[i]) for i in range(0, 11, 2))
    even_sum = sum(int(upc12[i]) for i in range(1, 10, 2))
    total    = (odd_sum * 3) + even_sum
    expected = (10 - (total % 10)) % 10
    return int(upc12[11]) == expected


def _fetch_off(upc: str) -> dict:
    """Return a slim dict from Open Food Facts, or error info on failure."""
    try:
        r = requests.get(_OFF_BASE.format(upc=upc), timeout=_REQUEST_TIMEOUT)
        data = r.json()
        if data.get("status") == 1:
            p = data.get("product", {})
            return {
                "off_found": True,
                "off_product_name": p.get("product_name", ""),
                "off_brand": p.get("brands", ""),
                "off_barcode": p.get("code", upc),
            }
        return {"off_found": False, "off_product_name": "", "off_brand": "", "off_barcode": ""}
    except Exception as exc:
        return {"off_found": None, "off_product_name": f"ERROR: {exc}", "off_brand": "", "off_barcode": ""}


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

    st.title("🔬 Chainlink UPC Diagnostic Tool")
    st.caption(
        "Chainlink AI validates every product UPC in your catalog — normalizing formats, "
        "verifying GS1 check digits, and confirming each barcode against the Open Food Facts "
        "database to catch bad data before it causes silent gap report failures."
    )

    if not conn or not tenant_id:
        st.error("No active tenant connection.")
        return

    # ------------------------------------------------------------------
    # Controls
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Validation (only runs when button is clicked)
    # ------------------------------------------------------------------
    if run_clicked:
        st.session_state.validation_check_off = check_off

        with st.spinner("Loading products from Snowflake…"):
            df = _load_products(conn, tenant_id)

        if df.empty:
            st.warning("No products found for this tenant.")
            st.session_state.validation_results = None
        else:
            if check_off:
                st.info(f"Found **{len(df):,}** products. Querying barcode database — this may take a minute.")
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
                    time.sleep(0.3)
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

    # ------------------------------------------------------------------
    # Display (reads from session state — survives reruns)
    # ------------------------------------------------------------------
    if st.session_state.validation_results is None:
        return

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
        db_not_found = results_df["BARCODE_DB_FOUND"].eq(False).sum()
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total Products",             total_products)
        c2.metric("✅ Valid UPCs",              int(valid_upc_count - corrected_count))
        c3.metric("🔧 Check Digit Corrected",   int(corrected_count))
        c4.metric("❌ Not Found in Barcode DB", int(db_not_found))
        c5.metric("⚠️ Blank / Null UPC",       int(blank_upc_count))
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
            | ~results_df["BARCODE_DB_FOUND"].eq(True)
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
