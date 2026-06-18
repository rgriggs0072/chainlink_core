# app_pages/upc_validation_test.py
"""
Admin-only UPC Validation Test Page

Pulls all products from PRODUCTS, normalizes each CARRIER_UPC, validates the
GS1 check digit, then hits Open Food Facts to confirm the barcode is live.

Temporary diagnostic tool — remove or gate behind a feature flag once the
underlying PRODUCTS data quality is confirmed clean.
"""

import time
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

    st.title("🔬 UPC Validation Test")
    st.caption(
        "Pulls every product from PRODUCTS, normalizes the CARRIER_UPC, "
        "validates the GS1 check digit, and confirms the barcode against "
        "Open Food Facts. Temporary diagnostic tool."
    )

    if not conn or not tenant_id:
        st.error("No active tenant connection.")
        return

    if not st.button("▶ Run Validation", type="primary", width="stretch"):
        return

    # ------------------------------------------------------------------
    # 1. Load products
    # ------------------------------------------------------------------
    with st.spinner("Loading products from Snowflake…"):
        df = _load_products(conn, tenant_id)

    if df.empty:
        st.warning("No products found for this tenant.")
        return

    st.info(f"Found **{len(df):,}** products. Querying Open Food Facts — this may take a minute.")

    # ------------------------------------------------------------------
    # 2. Normalize + validate + hit OFF
    # ------------------------------------------------------------------
    results = []
    progress = st.progress(0, text="Starting…")
    total = len(df)

    for i, row in enumerate(df.itertuples(index=False), start=1):
        raw_upc       = str(row.CARRIER_UPC) if row.CARRIER_UPC is not None else ""
        normalized    = normalize_upc(raw_upc) or ""
        upc_12        = calculate_upc_check_digit(normalized) if normalized else ""
        check_ok      = _verify_check_digit(upc_12)

        off = _fetch_off(upc_12) if upc_12 else {
            "off_found": False, "off_product_name": "", "off_brand": "", "off_barcode": ""
        }

        results.append({
            "PRODUCT_ID":       row.PRODUCT_ID,
            "SUPPLIER":         row.SUPPLIER,
            "PRODUCT_NAME":     row.PRODUCT_NAME,
            "PACKAGE":          row.PACKAGE,
            "RAW_UPC":          raw_upc,
            "NORMALIZED_UPC":   normalized,
            "UPC_12":           upc_12,
            "CHECK_DIGIT_OK":   check_ok,
            "OFF_FOUND":        off["off_found"],
            "OFF_PRODUCT_NAME": off["off_product_name"],
            "OFF_BRAND":        off["off_brand"],
            "OFF_BARCODE":      off["off_barcode"],
        })

        progress.progress(i / total, text=f"{i}/{total} — {row.PRODUCT_NAME[:40]}")
        time.sleep(0.3)

    progress.empty()
    results_df = pd.DataFrame(results)

    # ------------------------------------------------------------------
    # 3. Summary stats
    # ------------------------------------------------------------------
    total_products  = len(results_df)
    check_ok_count  = results_df["CHECK_DIGIT_OK"].sum()
    check_bad_count = total_products - check_ok_count
    off_found       = results_df["OFF_FOUND"].eq(True).sum()
    off_not_found   = results_df["OFF_FOUND"].eq(False).sum()
    off_error       = results_df["OFF_FOUND"].isna().sum()

    st.markdown("---")
    st.subheader("Summary")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Products",   total_products)
    c2.metric("Check Digit ✅",   int(check_ok_count))
    c3.metric("Check Digit ❌",   int(check_bad_count))
    c4.metric("Found on OFF ✅",  int(off_found))
    c5.metric("Not on OFF ❌",    int(off_not_found))

    if off_error:
        st.warning(f"{off_error} product(s) returned an API error — see OFF_FOUND = None rows.")

    # ------------------------------------------------------------------
    # 4. Results table
    # ------------------------------------------------------------------
    st.markdown("---")
    st.subheader("Results")

    # Highlight problem rows for quick scanning
    problem_df = results_df[~results_df["CHECK_DIGIT_OK"] | ~results_df["OFF_FOUND"].eq(True)]
    if not problem_df.empty:
        with st.expander(f"⚠️ {len(problem_df)} problem rows (bad check digit or not on OFF)", expanded=True):
            st.dataframe(problem_df, width="stretch", hide_index=True)

    st.dataframe(results_df, width="stretch", hide_index=True)

    # ------------------------------------------------------------------
    # 5. Download
    # ------------------------------------------------------------------
    st.download_button(
        label="📥 Download Full Results (Excel)",
        data=_to_excel(results_df),
        file_name="upc_validation_results.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="stretch",
    )
