# utils/validation_helpers.py
"""
Generic DataFrame validation helpers for Chainlink Core uploads.

Overview:
- ColumnRule: describes one column (name, dtype, required, custom validators).
- ValidationResult: holds cleaned DataFrame + errors + warnings.
- validate_dataframe(): core engine that:
    * checks required columns
    * drops extra columns (with a warning)
    * coerces dtypes
    * runs per-column validators

Use:
- Each upload type (CUSTOMERS, SALES, RESET_SCHEDULE, DISTRO_GRID, etc.)
  defines its own schema: List[ColumnRule].
- Call validate_dataframe(formatted_df, schema) in each formatter.
"""

from dataclasses import dataclass, field
from typing import Callable, Any, List, Dict, Optional
import pandas as pd


@dataclass
class ColumnRule:
    """
    Defines validation rules for a single column in an upload template.

    Attributes:
        name: Column name expected in the DataFrame (after formatting/renaming).
        required: If True, column must exist in the DataFrame.
        dtype: Target dtype: 'str', 'int', 'float', 'date'.
        allow_blank: If False, missing/invalid values become errors.
        validators: List of callables(series) -> List[str] (error messages).
    """
    name: str
    required: bool = True
    dtype: str = "str"  # 'str', 'int', 'float', 'date'
    allow_blank: bool = False
    validators: List[Callable[[pd.Series], List[str]]] = field(default_factory=list)


@dataclass
class ValidationResult:
    """
    Result of validate_dataframe().

    Attributes:
        cleaned_df: DataFrame after coercion and trimming, or None on fatal errors.
        errors: Hard-stop issues; upload should NOT proceed if non-empty.
        warnings: Non-fatal issues; show to user but allow upload if they accept.
    """
    cleaned_df: Optional[pd.DataFrame]
    errors: List[str]
    warnings: List[str]


def _coerce_dtype(series: pd.Series, dtype: str, col_name: str, errors: List[str]) -> pd.Series:
    """
    Attempt to coerce a Series to the requested dtype.
    - For 'str', use pandas StringDtype so missing values stay as <NA>, not literal 'nan'.
    """
    if dtype == "str":
        # Use pandas nullable StringDtype to preserve missing values
        try:
            s = series.astype("string")
        except TypeError:
            # Fallback for older pandas versions
            s = series.astype("string[python]")
        return s.str.strip()

    if dtype == "int":
        # Use pandas nullable Int64; invalids become <NA>
        try:
            return pd.to_numeric(series, errors="coerce").astype("Int64")
        except Exception:
            errors.append(f"Column '{col_name}': could not convert to integer.")
            return pd.to_numeric(series, errors="coerce")

    if dtype == "float":
        try:
            return pd.to_numeric(series, errors="coerce")
        except Exception:
            errors.append(f"Column '{col_name}': could not convert to numeric.")
            return pd.to_numeric(series, errors="coerce")

    if dtype == "date":
        try:
            return pd.to_datetime(series, errors="coerce").dt.date
        except Exception:
            errors.append(f"Column '{col_name}': could not convert to date.")
            return pd.to_datetime(series, errors="coerce")

    # Fallback: leave as-is if dtype is unknown
    return series



def validate_dataframe(df: pd.DataFrame, schema: List[ColumnRule]) -> ValidationResult:
    """
    Validate a DataFrame against a schema.

    Steps:
        1) Ensure all required columns exist.
        2) Warn and drop unexpected columns.
        3) Coerce each column to its target dtype.
        4) Enforce non-blank rules for required columns.
        5) Run any custom validators per column.

    Returns:
        ValidationResult with:
            - cleaned_df: None if there are fatal errors.
            - errors: list of fatal validation messages.
            - warnings: non-fatal messages (e.g. extra columns dropped).
    """
    errors: List[str] = []
    warnings: List[str] = []
    df = df.copy()

    expected_cols = [c.name for c in schema]

    # 1) Required columns present?
    missing = [c.name for c in schema if c.required and c.name not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {', '.join(missing)}")
        return ValidationResult(cleaned_df=None, errors=errors, warnings=warnings)

    # 2) Drop extra columns, but tell the user
    extra = [c for c in df.columns if c not in expected_cols]
    if extra:
        warnings.append(f"Ignoring unexpected columns: {', '.join(extra)}")
        df = df[expected_cols]

    # 3) Per-column processing
    for col_rule in schema:
        col = col_rule.name
        if col not in df.columns:
            # Non-required, missing column is fine; skip
            continue

        series = df[col]

        # Coerce dtype
        series = _coerce_dtype(series, col_rule.dtype, col, errors)

        # Required + not-blank enforcement
        if col_rule.required and not col_rule.allow_blank:
            if series.isna().any():
                null_count = int(series.isna().sum())
                errors.append(f"Column '{col}' has {null_count} missing/invalid values.")

        # Run any custom validators (each returns a list of error strings)
        for validator in col_rule.validators:
            col_errors = validator(series)
            errors.extend(col_errors)

        df[col] = series

    cleaned = df if not errors else None
    return ValidationResult(cleaned_df=cleaned, errors=errors, warnings=warnings)
