# ---------- app_pages/gap_history.py ----------
"""
Gap History & Streaks

Overview for future devs:
- Shows current gap streaks using GAP_CURRENT_STREAKS (built from GAP_REPORT_SNAPSHOT).
- Adds snapshot visibility + publishing on THIS page to avoid "filtered publish" mistakes.

Key UI sections:
1) Snapshot Status + Publish (admin only)
2) Streak Filters (form) + Table
3) Exports (CSV + PDF)

Hard rules:
- Publishing ALWAYS runs with "All" filters to snapshot full tenant-wide data.
- Never publish whatever the user filtered on-screen.

Data contract:
- This page uses fetch_current_streaks() (shared with email path) so preview/download/email stay consistent.
- Address must be available as ADDRESS (normalized here if upstream uses different casing).
"""

from __future__ import annotations

import os
from typing import Optional, Tuple, List

import pandas as pd
import streamlit as st

from utils.pdf_reports import build_gap_streaks_pdf, GAP_HISTORY_PDF_COLUMNS
from utils.gap_history_helpers import save_gap_snapshot, normalize_upc
from utils.reports_utils import create_gap_report
from utils.gap_history_mailer import fetch_current_streaks


# Optional admin gate (use if you have it; otherwise we fall back gracefully)
try:
    from utils.auth_utils import is_admin_user
except Exception:
    is_admin_user = None

    
# ----------------------------------------------------------------------
# Tenant helpers
# ----------------------------------------------------------------------
def _get_tenant_id() -> Optional[int]:
    """Extract tenant_id from st.session_state['tenant_config'] (dict or object)."""
    tenant_config = st.session_state.get("tenant_config")
    if tenant_config is None:
        return None
    if isinstance(tenant_config, dict):
        return tenant_config.get("tenant_id")
    return getattr(tenant_config, "tenant_id", None)


def _get_tenant_name_fallback() -> str:
    """Best-effort tenant display name for PDFs and UI."""
    tenant_config = st.session_state.get("tenant_config") or {}
    if isinstance(tenant_config, dict):
        return tenant_config.get("display_name") or tenant_config.get("tenant_name") or "Client"
    return (
        getattr(tenant_config, "display_name", None)
        or getattr(tenant_config, "tenant_name", None)
        or "Client"
    )


# ----------------------------------------------------------------------
# Snapshot status + publish
# ----------------------------------------------------------------------
def _get_week_start(d: pd.Timestamp) -> pd.Timestamp:
    """Return Monday (ISO week start) for a given timestamp."""
    return (d - pd.Timedelta(days=d.weekday())).normalize()


def fetch_snapshot_status(conn, tenant_id: int) -> pd.DataFrame:
    """
    Pull latest runs for the tenant from GAP_REPORT_RUNS.

    Returns a DF with columns:
      SNAPSHOT_WEEK_START, RUN_AT, TRIGGERED_BY, ROW_COUNT
    """
    sql = """
        SELECT
            SNAPSHOT_WEEK_START,
            RUN_AT,
            TRIGGERED_BY,
            ROW_COUNT
        FROM GAP_REPORT_RUNS
        WHERE TENANT_ID = %s
        ORDER BY SNAPSHOT_WEEK_START DESC, RUN_AT DESC
        LIMIT 20
    """
    cur = conn.cursor()
    try:
        cur.execute(sql, (int(tenant_id),))
        rows = cur.fetchall()
        if not rows:
            return pd.DataFrame()
        cols = [c[0] for c in cur.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()


def _user_is_admin(tenant_id: int) -> bool:
    """
    Page-level helper.
    Determines whether the logged-in user is an ADMIN for this tenant.
    """
    user_email = st.session_state.get("user_email") or st.session_state.get("username") or ""
    if not user_email:
        return False

    if is_admin_user is None:
        return False

    try:
        return bool(is_admin_user(user_email, str(tenant_id)))
    except Exception:
        return False


def build_snapshot_df_from_gap_report(df_gaps: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the Excel gap report DF into the snapshot DF we persist.

    Important:
    - Normalizes UPC and SR_UPC through normalize_upc() to prevent '.0' artifacts.
    - IS_GAP is TRUE when SR_UPC is missing/blank (meaning no sales).
    """
    snapshot_df = pd.DataFrame()

    snapshot_df["CHAIN_NAME"] = df_gaps.get("CHAIN_NAME")
    snapshot_df["STORE_NUMBER"] = df_gaps.get("STORE_NUMBER")
    snapshot_df["STORE_NAME"] = df_gaps.get("STORE_NAME")

    snapshot_df["SUPPLIER_NAME"] = df_gaps.get("SUPPLIER")
    snapshot_df["PRODUCT_NAME"] = df_gaps.get("PRODUCT_NAME")
    snapshot_df["SALESPERSON_NAME"] = df_gaps.get("SALESPERSON")

    # dg_upc -> UPC (schematic key)
    snapshot_df["UPC"] = df_gaps["dg_upc"].apply(normalize_upc) if "dg_upc" in df_gaps.columns else None

    # sr_upc -> SR_UPC (sales key)
    snapshot_df["SR_UPC"] = df_gaps["sr_upc"].apply(normalize_upc) if "sr_upc" in df_gaps.columns else None

    # IN_SCHEMATIC (best-effort)
    snapshot_df["IN_SCHEMATIC"] = df_gaps["In_Schematic"] if "In_Schematic" in df_gaps.columns else True

    sr = snapshot_df["SR_UPC"]
    snapshot_df["IS_GAP"] = sr.isna() | (sr.astype(str).str.strip() == "")

    snapshot_df["GAP_CASES"] = None
    snapshot_df["LAST_PURCHASE_DATE"] = None
    snapshot_df["CATEGORY"] = None
    snapshot_df["SUBCATEGORY"] = None

    return snapshot_df


def publish_weekly_snapshot_all(conn, tenant_id: int) -> Tuple[bool, str]:
    """
    Publish tenant-wide weekly snapshot using UNFILTERED gap report generation.

    Returns (success, message).
    """
    triggered_by = st.session_state.get("user_email") or st.session_state.get("username") or "gap_history_publish"
    snapshot_week_start = _get_week_start(pd.Timestamp.utcnow().normalize())

    temp_file_path = None
    try:
        # Always publish UNFILTERED
        temp_file_path = create_gap_report(conn, "All", "All", "All")
        if not temp_file_path or not os.path.exists(temp_file_path):
            return False, "Report generation failed (no file returned)."

        df_gaps = pd.read_excel(temp_file_path, engine="openpyxl")
        if df_gaps is None or df_gaps.empty:
            return False, "Generated report was empty; nothing to snapshot."

        snapshot_df = build_snapshot_df_from_gap_report(df_gaps)

        saved = save_gap_snapshot(
            conn=conn,
            tenant_id=int(tenant_id),
            df_gaps=snapshot_df,
            snapshot_week_start=snapshot_week_start,
            triggered_by=triggered_by,
        )

        if saved:
            return True, "✅ Published weekly gap snapshot (tenant-wide)."
        return False, "Snapshot already exists for this week (skipped)."

    except Exception as e:
        return False, f"Publish failed: {e}"

    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except Exception:
                pass


# ----------------------------------------------------------------------
# Styling helpers
# ----------------------------------------------------------------------
def _assign_streak_color(streak_weeks: int) -> str:
    """1 -> '', 2 -> yellow, 3 -> orange, 4+ -> red."""
    if streak_weeks >= 4:
        return "red"
    if streak_weeks == 3:
        return "orange"
    if streak_weeks == 2:
        return "yellow"
    return ""


def _style_gap_table(df: pd.DataFrame):
    """
    Apply row highlight using a helper column named _STREAK_COLOR.
    We keep the helper col in the DF for styling, then hide it.
    """
    def row_style(row):
        color = row.get("_STREAK_COLOR", "")
        if color == "red":
            return ["background-color: #ffb3b3"] * len(row)
        if color == "orange":
            return ["background-color: #ffd9b3"] * len(row)
        if color == "yellow":
            return ["background-color: #fff7b3"] * len(row)
        return [""] * len(row)

    styler = df.style.apply(row_style, axis=1)
    try:
        styler = styler.hide(columns=["_STREAK_COLOR"])
    except Exception:
        pass
    return styler


def _normalize_address_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize address column naming to ADDRESS.

    Why:
    - Some upstream paths may return Address/address.
    - PDF builder expects ADDRESS.
    """
    if df is None or df.empty:
        return df

    if "ADDRESS" in df.columns:
        return df

    for alt in ("Address", "address"):
        if alt in df.columns:
            return df.rename(columns={alt: "ADDRESS"})

    # Ensure it exists to avoid KeyErrors in slicing and keep PDF consistent
    out = df.copy()
    out["ADDRESS"] = ""
    return out


def _safe_select(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    """Select only columns that exist (prevents KeyError + accidental drops)."""
    existing = [c for c in cols if c in df.columns]
    return df[existing].copy()


# ----------------------------------------------------------------------
# Page render
# ----------------------------------------------------------------------
def render():
    st.title("Gap History & Streaks (By Salesperson)")

    # ------------------------------------------------------------------
    # Metric cards CSS (must be inside render)
    # ------------------------------------------------------------------
    st.markdown(
        """
<style>
.snapshot-cards {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px;
  margin-top: 6px;
  margin-bottom: 6px;
}
.snapshot-card {
  background: #F8F2EB;
  border: 1px solid rgba(0,0,0,0.06);
  border-radius: 14px;
  padding: 14px 14px 10px 14px;
  box-shadow: 0 2px 10px rgba(0,0,0,0.05);
}
.snapshot-label {
  font-size: 0.85rem;
  opacity: 0.75;
  margin-bottom: 6px;
}
.snapshot-value {
  font-size: 1.35rem;
  font-weight: 700;
  line-height: 1.1;
}
@media (max-width: 900px) {
  .snapshot-cards { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
</style>
""",
        unsafe_allow_html=True,
    )

    # ------------------------------------------------------------------
    # Connection + tenant context
    # ------------------------------------------------------------------
    conn = st.session_state.get("conn")
    if not conn:
        st.error("❌ Database connection not available.")
        return

    tenant_id = _get_tenant_id()
    if tenant_id is None:
        st.error("❌ tenant_id not found in session_state. Cannot load gap history.")
        return

    tenant_name = _get_tenant_name_fallback()

    # ------------------------------------------------------------------
    # Snapshot status + publish
    # ------------------------------------------------------------------
    with st.spinner("Checking snapshot status…"):
        runs_df = fetch_snapshot_status(conn, int(tenant_id))

    this_week = _get_week_start(pd.Timestamp.utcnow().normalize()).date()

    latest_week = None
    latest_row_count = None
    latest_run_at = None
    latest_triggered_by = None

    if not runs_df.empty:
        runs_df["SNAPSHOT_WEEK_START"] = pd.to_datetime(runs_df["SNAPSHOT_WEEK_START"]).dt.date
        latest = runs_df.iloc[0]
        latest_week = latest["SNAPSHOT_WEEK_START"]
        latest_row_count = latest.get("ROW_COUNT")
        latest_run_at = latest.get("RUN_AT")
        latest_triggered_by = latest.get("TRIGGERED_BY")

    published_this_week = (latest_week == this_week)

    st.markdown("### Weekly Snapshot Status")
    st.markdown(
        f"""
<div class="snapshot-cards">
  <div class="snapshot-card">
    <div class="snapshot-label">This week (Mon)</div>
    <div class="snapshot-value">{str(this_week)}</div>
  </div>
  <div class="snapshot-card">
    <div class="snapshot-label">Published?</div>
    <div class="snapshot-value">{"YES" if published_this_week else "NO"}</div>
  </div>
  <div class="snapshot-card">
    <div class="snapshot-label">Latest week</div>
    <div class="snapshot-value">{str(latest_week) if latest_week else "—"}</div>
  </div>
  <div class="snapshot-card">
    <div class="snapshot-label">Latest rows</div>
    <div class="snapshot-value">{str(latest_row_count) if latest_row_count is not None else "—"}</div>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )

    if latest_run_at or latest_triggered_by:
        st.caption(f"Latest run at: {latest_run_at} | Triggered by: {latest_triggered_by}")

    # Publish button (admin only) — show only when missing
    is_admin = _user_is_admin(int(tenant_id))

    if not published_this_week:
        if is_admin:
            with st.expander("📌 Publish Weekly Snapshot (Admin)", expanded=True):
                st.warning(
                    "This publishes a tenant-wide snapshot using **All Chains / All Salespeople / All Suppliers** "
                    "(no filters)."
                )
                confirm = st.checkbox("I understand this publishes ALL chains (not my filters).", value=False)
                if st.button("Publish Weekly Gap Snapshot (All Chains)", disabled=not confirm):
                    with st.spinner("Publishing snapshot…"):
                        ok, msg = publish_weekly_snapshot_all(conn, int(tenant_id))
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.info(msg)
        else:
            st.info("Snapshot is missing for this week. An admin can publish it from this page.")

    st.markdown("---")

    # ------------------------------------------------------------------
    # Load streak data (shared data path with email)
    # ------------------------------------------------------------------
    with st.spinner("Loading gap streaks…"):
        df = fetch_current_streaks(conn=conn, tenant_id=int(tenant_id))

    if df is None or df.empty:
        st.info("No gap history found yet. Publish at least one weekly snapshot to start tracking.")
        return

    df = _normalize_address_column(df)

    for col in ["SNAPSHOT_WEEK_START", "FIRST_GAP_WEEK", "LAST_GAP_WEEK"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col]).dt.date

    # ------------------------------------------------------------------
    # Filters (FORM to reduce reruns)
    # ------------------------------------------------------------------
    st.subheader("Current Gap Streaks by Salesperson")

    salespeople = sorted(df["SALESPERSON_NAME"].dropna().unique().tolist())
    chains = sorted(df["CHAIN_NAME"].dropna().unique().tolist())
    suppliers = sorted(df["SUPPLIER_NAME"].dropna().unique().tolist())

    salespeople.insert(0, "All")
    chains.insert(0, "All")
    suppliers.insert(0, "All")

    max_streak = int(pd.to_numeric(df["STREAK_WEEKS"], errors="coerce").fillna(1).max() or 1)

    with st.form("gap_history_filters", clear_on_submit=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            salesperson_filter = st.selectbox("Salesperson", salespeople, index=0)
        with c2:
            chain_filter = st.selectbox("Chain", chains, index=0)
        with c3:
            supplier_filter = st.selectbox("Supplier", suppliers, index=0)

        min_streak = st.slider(
            "Minimum streak length (weeks)",
            min_value=1,
            max_value=max_streak,
            value=1,
            help="Show items that have been gaps for at least this many consecutive weeks.",
        )

        st.form_submit_button("Apply Filters")

    filtered = df.copy()

    if salesperson_filter != "All":
        filtered = filtered[filtered["SALESPERSON_NAME"] == salesperson_filter]
    if chain_filter != "All":
        filtered = filtered[filtered["CHAIN_NAME"] == chain_filter]
    if supplier_filter != "All":
        filtered = filtered[filtered["SUPPLIER_NAME"] == supplier_filter]

    filtered["STREAK_WEEKS"] = pd.to_numeric(filtered["STREAK_WEEKS"], errors="coerce").fillna(0).astype(int)
    filtered = filtered[filtered["STREAK_WEEKS"] >= int(min_streak)]

    if filtered.empty:
        st.warning("No gaps match the selected filters and minimum streak length.")
        return

    filtered["_STREAK_COLOR"] = filtered["STREAK_WEEKS"].apply(_assign_streak_color)

    display_cols = [
        "STREAK_WEEKS",
        "SNAPSHOT_WEEK_START",
        "FIRST_GAP_WEEK",
        "LAST_GAP_WEEK",
        "SALESPERSON_NAME",
        "CHAIN_NAME",
        "STORE_NUMBER",
        "STORE_NAME",
        "ADDRESS",
        "SUPPLIER_NAME",
        "PRODUCT_NAME",
        "UPC",
        "_STREAK_COLOR",
    ]

    display_df = _safe_select(filtered, display_cols).sort_values(
        by=["SALESPERSON_NAME", "STREAK_WEEKS", "CHAIN_NAME", "STORE_NUMBER", "PRODUCT_NAME"],
        ascending=[True, False, True, True, True],
    )

    styled = _style_gap_table(display_df)
    st.dataframe(styled, width="stretch")
    st.write(f"Showing {len(display_df)} gap streak(s).")

    # ------------------------------------------------------------------
    # Downloads
    # ------------------------------------------------------------------
    csv_df = display_df.drop(columns=["_STREAK_COLOR"], errors="ignore")
    csv_bytes = csv_df.to_csv(index=False).encode("utf-8")

    st.download_button(
        label="📥 Download Gap Streaks (CSV)",
        data=csv_bytes,
        file_name="gap_streaks_by_salesperson.csv",
        mime="text/csv",
        key="gap_streaks_csv",
    )

   # Canonical PDF contract lives in utils/pdf_reports.py
    pdf_df = _safe_select(csv_df, GAP_HISTORY_PDF_COLUMNS)


    DEBUG_PDF = False

    if DEBUG_PDF:
        st.write("DOWNLOAD pdf_df columns:", pdf_df.columns.tolist())
        st.write(
            "DOWNLOAD ADDRESS sample:",
            pdf_df["ADDRESS"].head(10).tolist() if "ADDRESS" in pdf_df.columns else "NO ADDRESS COLUMN",
        )


    pdf_bytes = build_gap_streaks_pdf(
        pdf_df,
        tenant_name=tenant_name,
        salesperson_name=None,  # page is multi-salesperson; keep header clean
    )

    st.download_button(
        label="📄 Download Gap Streaks (PDF)",
        data=pdf_bytes,
        file_name="gap_streaks_report.pdf",
        mime="application/pdf",
        key="gap_streaks_pdf",
    )
