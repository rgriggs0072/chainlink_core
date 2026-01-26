# utils/distro_grid/formatters.py
"""
Distro Grid Formatters

Overview for future devs:
- Converts raw Excel uploads into a normalized Distro Grid DataFrame.
- Focuses on mechanical cleaning only:
    * Standardizes headers
    * Cleans STORE_NUMBER / UPC / YES_NO
    * Injects CHAIN_NAME (UI selection)
- Generates on-the-fly template DataFrames for the UI to offer as downloads.

Important:
- SEASON is NOT part of the live DISTRO_GRID table. It belongs in
  DG_ARCHIVE_TRACKING and is determined at upload time (see infer_season_label
  in schema.py). The formatter does not include SEASON anymore.
"""

from typing import Literal
from io import BytesIO
import os

import pandas as pd
import streamlit as st


from .schema import UPLOAD_COLUMNS


UploadLayout = Literal["standard", "pivot"]


def _normalize_header(c) -> str:
    """
    Normalize Excel column headers so validation survives:
    - non-breaking spaces
    - wrapped headers (newlines)
    - repeated whitespace
    - leading/trailing whitespace
    - standardizes to UPPER_CASE_WITH_UNDERSCORES
    """
    s = str(c) if c is not None else ""
    s = s.replace("\u00A0", " ")          # non-breaking space from Excel exports
    s = s.replace("\n", " ").replace("\r", " ")
    s = " ".join(s.split())              # collapse repeated whitespace
    s = s.strip().upper().replace(" ", "_")
    return s



# ---------------------------------------------------------------------
# Template builders
# ---------------------------------------------------------------------

def build_standard_template_df() -> pd.DataFrame:
    """
    Build a minimal standard Distro Grid template DataFrame.

    Notes:
    - Includes CHAIN_NAME column so users can see it, but the app will
      overwrite it at runtime with the selected chain.
    - Does NOT include SEASON; season is inferred at upload time and only
      written into DG_ARCHIVE_TRACKING, not DISTRO_GRID.
    """
    cols = [
        "CHAIN_NAME",
        "STORE_NAME",
        "STORE_NUMBER",
        "COUNTY",
        "UPC",
        "SKU",
        "PRODUCT_NAME",
        "MANUFACTURER",
        "SEGMENT",
        "YES_NO",
        "ACTIVATION_STATUS",
    ]

    data = [{
        "CHAIN_NAME": "SAMPLE CHAIN NAME",
        "STORE_NAME": "SAMPLE STORE NAME",
        "STORE_NUMBER": 1234,
        "COUNTY": "",
        "UPC": "012345678901",
        "SKU": "",
        "PRODUCT_NAME": "Sample Product",
        "MANUFACTURER": "",
        "SEGMENT": "",
        "YES_NO": 1,
        "ACTIVATION_STATUS": "",
    }]

    return pd.DataFrame(data, columns=cols)


def build_standard_template_xlsx() -> BytesIO:
    """
    Build an in-memory .xlsx file for the standard Distro Grid template.

    Returns:
        BytesIO ready for Streamlit st.download_button(data=...).
    """
    df = build_standard_template_df()
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    buffer.seek(0)
    return buffer


def build_pivot_template_df() -> pd.DataFrame:
    """
    Build a pivot-style Distro Grid template DataFrame.

    Layout (matches legacy Pivot_Table_Distro_Grid_Template.xlsx):
    - UPC
    - SKU #
    - Name
    - Manufacturer
    - SEGMENT
    - 1, 2, 3, ..., 53  (store-number columns)

    Notes:
    - We don't try to be clever with store names here; we only model store
      numbers as columns. STORE_NAME is resolved later via CUSTOMERS.
    """
    id_cols = ["UPC", "SKU #", "Name", "Manufacturer", "SEGMENT"]
    store_cols = list(range(1, 54))  # 1..53

    cols = id_cols + store_cols

    # Single example row, mostly empty; users will overwrite.
    data = [{
        "UPC": "012345678901",
        "SKU #": "",
        "Name": "Sample Product Name",
        "Manufacturer": "Sample Manufacturer",
        "SEGMENT": "Sample Segment",
        **{c: "" for c in store_cols},
    }]

    return pd.DataFrame(data, columns=cols)


def build_pivot_template_xlsx() -> BytesIO:
    """
    Build an in-memory .xlsx file for the pivot-style Distro Grid template.

    Returns:
        BytesIO ready for Streamlit st.download_button(data=...).
    """
    df = build_pivot_template_df()
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)
    buffer.seek(0)
    return buffer


# ---------------------------------------------------------------------
# Formatter: raw Excel -> normalized DataFrame
# ---------------------------------------------------------------------

def format_uploaded_grid(
    df_raw: pd.DataFrame,
    layout: UploadLayout,
    chain_name: str,
) -> pd.DataFrame:
    """
    Format a raw Distro Grid upload into the canonical upload DataFrame.

    Args:
        df_raw: DataFrame loaded from the Excel file.
        layout: "standard" or "pivot".
        chain_name: Chain name from UI; normalized to UPPERCASE.

    Returns:
        Formatted DataFrame with:
        - Normalized headers.
        - Cleaned STORE_NUMBER, UPC, YES_NO.
        - CHAIN_NAME column set from UI.
        - All required upload columns present (filled with NA if missing).
    """
    df = df_raw.copy()

    # Normalize headers: UPPER_CASE_WITH_UNDERSCORES
    df.columns = [_normalize_header(c) for c in df.columns]


    if layout == "standard":
        df = _format_standard(df)
    else:
        df = _format_pivot(df)

    # UI wins: inject CHAIN_NAME from controls
    df["CHAIN_NAME"] = chain_name.strip().upper()

    # Ensure all required upload columns exist
    for col_name, spec in UPLOAD_COLUMNS.items():
        if spec.required_upload and col_name not in df.columns:
            df[col_name] = pd.NA

    # Stable column order (only columns that actually exist in df)
    ordered_cols = [name for name in UPLOAD_COLUMNS.keys() if name in df.columns]
    df = df[ordered_cols]

    # Clean up NaNs for nicer Excel output
    for col in df.columns:
        if df[col].dtype == "object":
            # Fill NaNs and remove literal "nan"/"NaN"/"NAN"
            df[col] = (
                df[col]
                .fillna("")
                .replace(r"(?i)^nan$", "", regex=True)
            )

    return df


def _format_standard(df: pd.DataFrame) -> pd.DataFrame:
    """
    Format a non-pivot Distro Grid upload.

    - Cleans STORE_NAME apostrophes/smart quotes and strips trailing ' #<digits>'.
    - Cleans UPC hyphens.
    - Normalizes STORE_NUMBER to digits only (Int64).
    - Casts YES_NO into 0/1 (Int64) from common representations.
    - Strips whitespace from string columns and converts literal 'nan' to ''.
    """
    df = df.copy()

    # Normalize STORE_NAME: remove apostrophes, smart quotes, and trailing " #<digits>"
    if "STORE_NAME" in df.columns:
        smart_quote = "\u2019"
        df["STORE_NAME"] = (
            df["STORE_NAME"]
            .astype(str)
            .str.strip()
            .str.replace("'", "", regex=False)
            .str.replace(smart_quote, "", regex=False)
            # remove patterns like "WHOLE FOODS #10548" → "WHOLE FOODS"
            .str.replace(r"\s*#\s*\d+$", "", regex=True)
        )

    # Clean UPC hyphens
    if "UPC" in df.columns:
        df["UPC"] = df["UPC"].astype(str).str.strip().str.replace("-", "", regex=False)

    # STORE_NUMBER: extract digits from values like "TARGET #1862"
    if "STORE_NUMBER" in df.columns:
        df["STORE_NUMBER"] = (
            df["STORE_NUMBER"]
            .astype(str)
            .str.extract(r"(\d+)", expand=False)
            .replace("", pd.NA)
            .astype("Int64")
        )

    # YES_NO normalization (handles 1/0, Y/N, YES/NO)
    if "YES_NO" in df.columns:
        yes_no_map = {
            "1": 1,
            "0": 0,
            "Y": 1,
            "N": 0,
            "YES": 1,
            "NO": 0,
        }
        df["YES_NO"] = (
            df["YES_NO"]
            .astype(str)
            .str.strip()
            .str.upper()
            .map(yes_no_map)
        )
        df["YES_NO"] = df["YES_NO"].astype("Int64")

    # Strip whitespace + clean literal "nan" for all object columns
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = (
                df[col]
                .astype(str)
                .str.strip()
                .replace(r"(?i)^nan$", "", regex=True)
            )

    return df


def _format_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Format a pivot-style Distro Grid upload into the canonical row-per-store layout.

    Input layout (after header normalization in format_uploaded_grid):
    - UPC
    - SKU_#
    - NAME
    - MANUFACTURER
    - SEGMENT
    - 1, 2, 3, ..., 53  (store-number columns)

    Output layout (columns, BEFORE CHAIN_NAME injection):
    - STORE_NAME     (blank; enriched later from CUSTOMERS)
    - STORE_NUMBER   (Int64)
    - COUNTY         (blank)
    - UPC
    - SKU
    - PRODUCT_NAME
    - MANUFACTURER
    - SEGMENT
    - YES_NO         (0/1, Int64)
    - ACTIVATION_STATUS (blank)
    """
    df = df.copy()
    st.write("DEBUG formatters.py path:", os.path.abspath(__file__))
    st.write("DEBUG _format_pivot() running")

    # Required ID columns in the pivot layout (normalized to uppercase/underscores)
    # RIGHT BEFORE validation
    st.write("DEBUG raw columns:", [repr(c) for c in df.columns.tolist()])

        # ------------------------------------------------------------------
    # Pivot header compatibility
    # ------------------------------------------------------------------
    # Some clients/templates use NAME instead of PRODUCT_NAME.
    # Normalize here so the rest of the pipeline is stable.
    if "NAME" in df.columns and "PRODUCT_NAME" not in df.columns:
        df.rename(columns={"NAME": "PRODUCT_NAME"}, inplace=True)

    # Some pivot exports might use SKU instead of SKU_#
    if "SKU" in df.columns and "SKU_#" not in df.columns:
        df.rename(columns={"SKU": "SKU_#"}, inplace=True)


    id_cols = ["UPC", "SKU_#", "PRODUCT_NAME", "MANUFACTURER", "SEGMENT"]
    for col in id_cols:
        if col not in df.columns:
            raise ValueError(f"Pivot upload is missing required column '{col}'")

    # Anything that is not one of the ID columns is treated as a store-number column
    store_cols = [c for c in df.columns if c not in id_cols]
    if not store_cols:
        raise ValueError("Pivot upload has no store-number columns (1, 2, 3, ...).")

    # Melt store-number columns into rows
    melted = pd.melt(
        df,
        id_vars=id_cols,
        value_vars=store_cols,
        var_name="STORE_NUMBER_RAW",
        value_name="YES_NO_RAW",
    )

    # STORE_NUMBER: convert header values (e.g. 1, "1") into integer store numbers
    melted["STORE_NUMBER"] = (
        melted["STORE_NUMBER_RAW"]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)
        .replace("", pd.NA)
        .astype("Int64")
    )

    # YES_NO: map any non-empty/non-zero indicator to 1, else 0
    def _yes_no_to_int(val):
        if pd.isna(val):
            return 0
        s = str(val).strip().upper()
        if s in ("1", "Y", "YES", "TRUE", "T", "X", "✓", "✔"):
            return 1
        if s in ("0", "N", "NO", "FALSE", "F"):
            return 0
        # For any weird non-empty markers, treat as 1.
        return 1

    melted["YES_NO"] = melted["YES_NO_RAW"].apply(_yes_no_to_int).astype("Int64")

    # Build canonical output frame
    out = pd.DataFrame()
    out["STORE_NAME"] = ""  # resolved later via CUSTOMERS
    out["STORE_NUMBER"] = melted["STORE_NUMBER"]

    # UPC cleanup
    out["UPC"] = (
        melted["UPC"]
        .astype(str)
        .str.strip()
        .str.replace("-", "", regex=False)
    )

    # SKU cleanup from SKU_#
    out["SKU"] = (
        melted["SKU_#"]
        .fillna("")
        .astype(str)
        .str.replace(r"\.0$", "", regex=True)
        .str.strip()
    )

    # Product + manufacturer + segment
    out["PRODUCT_NAME"] = melted["PRODUCT_NAME"].astype(str).str.strip()
    out["MANUFACTURER"] = melted["MANUFACTURER"].astype(str).str.strip()
    out["SEGMENT"] = melted["SEGMENT"].astype(str).str.strip()

    # County + activation status as blank placeholders
    out["COUNTY"] = ""
    out["ACTIVATION_STATUS"] = ""

    out["YES_NO"] = melted["YES_NO"]

    # Ensure column order matches the canonical upload schema (minus CHAIN_NAME which
    # is injected later by format_uploaded_grid)
    desired_order = [
        "STORE_NAME",
        "STORE_NUMBER",
        "COUNTY",
        "UPC",
        "SKU",
        "PRODUCT_NAME",
        "MANUFACTURER",
        "SEGMENT",
        "YES_NO",
        "ACTIVATION_STATUS",
    ]
    out = out[desired_order]

    return out
