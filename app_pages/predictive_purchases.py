# ------------------------------------------- predictive_purchases.py ------------------------------------------------------
"""
Predictive Purchases Page
- Admin uploads sales file → loads RAW → aggregates WEEKLY → runs forecasts.
- Accepts UPC (+ optional PRODUCT_ID) list; forecasts tenant-wide totals.
- Generates a branded PDF report.

Dev notes:
- Forecast form runs in isolation via st.form (prevents full-page rerun).
- Spinner + progress bar indicate forecast progress.
- Buttons for PDF/clear appear only after forecasts have run (df exists).
"""

import re
import pandas as pd
import streamlit as st
import altair as alt
from utils.sales_ingest import load_sales_file
from utils.forecasting import fetch_weekly_upc_rollup, forecast_units, infer_revenue
from utils.pdf_reports import build_predictive_purchases_pdf


# ---------------- Helper ----------------
def _parse_entries(text: str):
    """
    Accept lines like:
      810273030389
      810273030389, PROD_1001
    Return: [{"upc":"810273030389","product_id":"PROD_1001"|None}, ...]
    """
    items = []
    for line in text.splitlines():
        parts = [p.strip() for p in line.split(",") if p.strip()]
        if not parts:
            continue
        upc = re.sub(r"\D", "", parts[0])
        pid = parts[1] if len(parts) > 1 else None
        if upc:
            items.append({"upc": upc, "product_id": pid})
    return items


# ---------------- Main Render ----------------
def render():
    st.title("🔮 Predictive Purchases")
    st.caption("Upload sales → aggregate weekly → forecast next period by UPC + Product ID.")

    # ---------- Upload & Aggregate ----------
    horizon = st.slider("Forecast horizon (weeks)", 2, 8, 4)
    uploaded = st.file_uploader("Upload daily sales CSV/XLSX", type=["csv", "xlsx"])

    if uploaded and st.button("Load & Aggregate", type="primary"):
        try:
            import_id = load_sales_file(uploaded, source="CSV")
            st.success(f"Loaded & aggregated. Import ID: {import_id}")
            st.rerun()
        except Exception as e:
            st.error(f"Load failed: {e}")

    # ---------- Forecast Form ----------
    st.subheader("Forecast")
    with st.form("forecast_form", clear_on_submit=False):
        upc_text = st.text_area(
            "Enter UPCs (optionally include PRODUCT_ID after a comma, one per line)",
            value="810273030389, PROD_1001\n012345678905, PROD_2002",
            key="upc_input",
        )

        # Advanced controls: expose forecasting knobs
        with st.expander("Advanced"):
            min_points   = st.slider("Minimum weeks of history required", 2, 16, 6)
            allow_naive  = st.checkbox("Allow naive fallback (mean of last N weeks)", value=True)
            naive_window = st.slider("Naive window (weeks)", 2, 12, 4, disabled=not allow_naive)

        entries = _parse_entries(upc_text)
        run_forecast = st.form_submit_button("Run Forecasts")

    # ---------- Forecast Logic ----------
    if run_forecast and entries:
        rows = []
        with st.spinner("Running forecasts... this may take a minute ⏳"):
            progress = st.progress(0)
            try:
                for i, entry in enumerate(entries):
                    hist = fetch_weekly_upc_rollup(entry["upc"], entry["product_id"])
                    fc_units = forecast_units(
                        hist,
                        horizon,
                        min_points=min_points,
                        allow_naive=allow_naive,
                        naive_window=naive_window,
                    )

                    if fc_units.empty:
                        rows.append({
                            "UPC": entry["upc"],
                            "PRODUCT_ID": entry.get("product_id"),
                            "status": "insufficient history",
                        })
                    else:
                        fc = infer_revenue(hist, fc_units)
                        next_units = float(fc.tail(horizon)["yhat"].sum())
                        next_rev   = float(fc.tail(horizon)["revenue_hat"].fillna(0).sum())
                        rows.append({
                            "UPC": entry["upc"],
                            "PRODUCT_ID": entry.get("product_id"),
                            "Forecast_Units_Next_Period": round(next_units, 2),
                            "Forecast_Revenue_Next_Period": round(next_rev, 2),
                            "status": "ok"  # if you later tag naive, consider "ok (naive)"
                        })

                    progress.progress((i + 1) / max(1, len(entries)))

                df = pd.DataFrame(rows)
                st.session_state["forecast_summary"] = df

                if not df.empty:
                    ok_n = int((df.get("status", pd.Series([]))).astype(str).str.startswith("ok").sum())
                    st.success(f"✅ Forecasts completed. {ok_n} OK, {len(df) - ok_n} insufficient.")
                else:
                    st.warning("No forecasts could be generated. Check data availability.")

            except Exception as e:
                st.error(f"❌ Forecast run failed: {e}")

    # ---------- Display Results & Actions (only after forecasts exist) ----------
    df = st.session_state.get("forecast_summary")
    if isinstance(df, pd.DataFrame) and not df.empty:
        st.dataframe(df, use_container_width=True)

        # Totals (OK rows only)
        ok_mask = df["status"].astype(str).str.startswith("ok")
        ok_df = df[ok_mask] if "status" in df.columns else df
        if not ok_df.empty and {"Forecast_Units_Next_Period", "Forecast_Revenue_Next_Period"}.issubset(ok_df.columns):
            c1, c2 = st.columns(2)
            c1.metric("Projected Units (next period)", f"{ok_df['Forecast_Units_Next_Period'].sum():,.0f}")
            c2.metric("Projected Revenue (next period)", f"${ok_df['Forecast_Revenue_Next_Period'].sum():,.2f}")

        # Quick viz
        if "Forecast_Units_Next_Period" in df.columns:
            chart = (
                alt.Chart(ok_df if not ok_df.empty else df)
                .mark_bar()
                .encode(
                    x=alt.X("UPC:N", title="UPC"),
                    y=alt.Y("Forecast_Units_Next_Period:Q", title="Units (next period)"),
                    tooltip=[
                        "UPC",
                        "PRODUCT_ID",
                        "Forecast_Units_Next_Period",
                        "Forecast_Revenue_Next_Period",
                        "status",
                    ],
                )
                .properties(height=260)
            )
            st.altair_chart(chart, use_container_width=True)

        # --- Actions row (show only after forecasts have run) ---
        act_cols = st.columns([1.2, 1.2, 1.6, 6])
        with act_cols[0]:
            st.download_button(
                "⬇️ Download CSV",
                data=df.to_csv(index=False).encode("utf-8"),
                file_name="predictive_purchases_forecast.csv",
                mime="text/csv",
            )

        with act_cols[1]:
            if st.button("Generate PDF Report"):
                tenant_name = st.session_state.get("tenant_config", {}).get("tenant_name", "Tenant")
                # Build PDF from OK rows; if none, build header-only (empty table)
                cols = ["UPC", "PRODUCT_ID", "Forecast_Units_Next_Period", "Forecast_Revenue_Next_Period"]
                subset = ok_df[cols] if (not ok_df.empty and set(cols).issubset(ok_df.columns)) else pd.DataFrame(columns=cols)
                pdf_bytes = build_predictive_purchases_pdf(
                    tenant_name=tenant_name,
                    horizon_weeks=horizon,
                    summary_table=subset,
                )
                st.session_state["predictive_pdf"] = pdf_bytes
                st.success("✅ PDF generated successfully.")

        # PDF download (only shown after Generate)
        if "predictive_pdf" in st.session_state:
            st.download_button(
                label="📄 Download Predictive Purchases PDF",
                data=st.session_state["predictive_pdf"],
                file_name="Predictive_Purchases_Report.pdf",
                mime="application/pdf",
            )

        # Clear controls (only meaningful after a forecast ran)
        clr_cols = st.columns([1, 9])
        with clr_cols[0]:
            if st.button("Clear Results"):
                st.session_state.pop("forecast_summary", None)
                st.session_state.pop("predictive_pdf", None)
                st.rerun()
