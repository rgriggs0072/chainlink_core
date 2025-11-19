# --------------------------------------- forecasting.py --------------------------------------------
# --------------------------------------- Forecasting Utility ---------------------------------------
"""
Forecasting helpers (Prophet-first, XGBoost-ready).

Overview for devs:
- fetch_weekly_upc_rollup(): pulls tenant-wide weekly series by UPC (+ optional PRODUCT_ID).
- forecast_units(): runs Prophet if possible; otherwise falls back to a naive mean model.
- infer_revenue(): multiplies unit forecast by recent average price-per-unit (revenue/units).

Notes:
- History normalization: accepts ['ds','units','revenue'] or ['ds','y','revenue'].
- Weekly cadence is aligned to Mondays ('W-MON') for stable week boundaries.
- Keep functions small and documented for maintainability.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

# Prophet is optional; we fall back if it isn't installed or fails at runtime.
try:
    from prophet import Prophet  # if your env uses fbprophet, change import accordingly
except Exception:  # ImportError or other env issues
    Prophet = None  # type: ignore

from sf_connector.service_connector import connect_to_tenant_snowflake


# ----------------------------- Data Fetch -----------------------------------------------------------

def fetch_weekly_upc_rollup(upc: str, product_id: str | None = None) -> pd.DataFrame:
    """
    Returns weekly totals with columns: ['ds','units','revenue'] (all lowercase).
    """
    tenant = st.session_state["tenant_config"]
    conn = connect_to_tenant_snowflake(tenant)
    db, sch = tenant["database"], tenant["schema"]

    sqls = [
        f"""
        SELECT
          WEEK_START_DATE AS ds,
          SUM(TOTAL_UNITS)   AS units,
          SUM(TOTAL_REVENUE) AS revenue
        FROM {db}.{sch}.SALES_WEEKLY_UPC
        WHERE UPC = %s {{pid_clause}}
        GROUP BY 1
        ORDER BY 1
        """,
        f"""
        SELECT
          WEEK_START_DATE AS ds,
          SUM(TOTAL_UNITS)   AS units,
          SUM(TOTAL_REVENUE) AS revenue
        FROM {db}.{sch}.SALES_WEEKLY
        WHERE UPC = %s {{pid_clause}}
        GROUP BY 1
        ORDER BY 1
        """
    ]

    pid_clause = "AND PRODUCT_ID = %s" if product_id else ""
    params = [upc] + ([product_id] if product_id else [])

    try:
        for tmpl in sqls:
            q = tmpl.format(pid_clause=f" {pid_clause}")
            try:
                df = pd.read_sql(q, conn, params=params)
                # 👇 normalize column names to lowercase (Snowflake returns upper-case)
                df.columns = [str(c).lower() for c in df.columns]
                return df
            except Exception:
                continue
        return pd.DataFrame()
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ----------------------------- Internal Helpers -----------------------------------------------------

def _normalize_hist(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize history to Prophet schema, case-insensitive:
      input:  ['ds','units','revenue'] OR ['ds','y','revenue'] (any case)
      output: ['ds','y',('revenue' if present)]
    """
    if df is None or df.empty:
        return pd.DataFrame()

    # 👇 lower-case all incoming columns for safety
    lower_map = {c: str(c).lower() for c in df.columns}
    out = df.rename(columns=lower_map).copy()

    if "ds" not in out.columns:
        raise KeyError("hist missing 'ds' column")

    if "y" not in out.columns:
        if "units" in out.columns:
            out = out.rename(columns={"units": "y"})
        else:
            raise KeyError("hist missing 'y' or 'units' column")

    out["ds"] = pd.to_datetime(out["ds"])
    keep = ["ds", "y"] + (["revenue"] if "revenue" in out.columns else [])
    return out[keep].dropna(subset=["ds", "y"]).sort_values("ds")



def _future_weeks(last_ds: pd.Timestamp, horizon: int) -> pd.DatetimeIndex:
    """
    Build a horizon-length weekly index starting the Monday after last_ds.
    """
    last_ds = pd.to_datetime(last_ds)
    # Move to next Monday from last_ds; then build weekly Mondays
    first_future = last_ds + pd.Timedelta(days=7)
    return pd.date_range(first_future, periods=horizon, freq="W-MON")


def _naive_mean_forecast(hist_y: pd.DataFrame, horizon: int, window: int = 4) -> pd.DataFrame:
    """
    Naive fallback: forecast equals the mean of the last `window` observed weeks.
    Returns ['ds','yhat','yhat_lower','yhat_upper'] for the next `horizon` weeks.
    """
    if hist_y is None or hist_y.empty:
        return pd.DataFrame()

    hist_y = hist_y[["ds", "y"]].dropna().sort_values("ds")
    if hist_y.empty:
        return pd.DataFrame()

    recent = hist_y.tail(window)["y"]
    if recent.empty:
        return pd.DataFrame()

    mean_val = float(recent.mean())
    future = _future_weeks(hist_y["ds"].max(), horizon)
    out = pd.DataFrame({
        "ds": future,
        "yhat": [mean_val] * horizon,
        "yhat_lower": [mean_val] * horizon,
        "yhat_upper": [mean_val] * horizon,
    })
    return out


# ----------------------------- Forecasting ----------------------------------------------------------

def forecast_units(
    hist: pd.DataFrame,
    horizon: int,
    *,
    min_points: int = 6,
    allow_naive: bool = True,
    naive_window: int = 4,
) -> pd.DataFrame:
    """
    Forecast weekly units for the next `horizon` weeks.

    Behavior:
    - Normalizes input history to ['ds','y'].
    - If unique weeks < min_points → naive fallback (if allowed) else empty.
    - If Prophet not installed or fit/predict fails → naive fallback (if allowed).
    - Returns ['ds','yhat','yhat_lower','yhat_upper'] for the future period only.
    """
    hist_y = _normalize_hist(hist)
    if hist_y.empty:
        return pd.DataFrame()

    if hist_y["ds"].nunique() < min_points:
        return _naive_mean_forecast(hist_y, horizon, naive_window) if allow_naive else pd.DataFrame()

    # Try Prophet; fall back to naive on any failure or if Prophet missing.
    if Prophet is None:
        return _naive_mean_forecast(hist_y, horizon, naive_window) if allow_naive else pd.DataFrame()

    try:
        m = Prophet(
            weekly_seasonality=True,
            daily_seasonality=False,
            yearly_seasonality=False,
            seasonality_mode="additive",
        )
        m.fit(hist_y[["ds", "y"]])
        future = m.make_future_dataframe(periods=horizon, freq="W-MON")
        fc = m.predict(future)[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(horizon)
        return fc
    except Exception:
        return _naive_mean_forecast(hist_y, horizon, naive_window) if allow_naive else pd.DataFrame()


def infer_revenue(hist: pd.DataFrame, fc_units: pd.DataFrame, recent_weeks: int = 8) -> pd.DataFrame:
    """
    Attach 'revenue_hat' to a unit forecast using a recent average price-per-unit.

    - Accepts hist with ['ds','units','revenue'] or ['ds','y','revenue'].
    - price_per_unit = SUM(revenue) / SUM(units) over the last `recent_weeks`.
    - If price cannot be inferred, defaults to 1.0.
    - Returns the same fc_units DataFrame with an added 'revenue_hat' column.
    """
    if fc_units is None or fc_units.empty:
        return pd.DataFrame()

    price = None
    if hist is not None and not hist.empty and "revenue" in hist.columns:
        h = _normalize_hist(hist)  # ensures we have ['ds','y'] and preserves 'revenue' if present
        if "revenue" in hist.columns:  # _normalize_hist keeps 'revenue' only if present
            # Use the original hist (not the normalized 'h') to preserve 'revenue' column
            h2 = hist.copy()
            if "units" not in h2.columns and "y" in h2.columns:
                h2 = h2.rename(columns={"y": "units"})
            if "units" in h2.columns:
                h2 = h2.dropna(subset=["units"]).sort_values("ds").tail(recent_weeks)
                total_units = float(h2["units"].sum())
                total_rev = float(h2["revenue"].sum())
                if total_units > 0:
                    price = total_rev / total_units

    if price is None:
        price = 1.0

    out = fc_units.copy()
    out["revenue_hat"] = out["yhat"] * float(price)
    return out
