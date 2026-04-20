# utils/distro_grid/schema.py
"""
Distro Grid Schema Definitions

Overview for future devs:
- Defines the logical upload schema (what we expect from a Distro Grid Excel).
- Documents the physical Snowflake table shapes for reference:
    DISTRO_GRID
    DISTRO_GRID_ARCHIVE_FULL     — full recovery backup (renamed from DISTRO_GRID_ARCHIVE in v1.2.0)
    DISTRO_GRID_MATCHED_ARCHIVE  — filtered archive for Placement Intelligence (new in v1.2.0)
    DG_ARCHIVE_TRACKING
    RESET_SCHEDULE (related)
- All formatters/validators should reference these definitions instead of
  hard-coding column names or valid values.

v1.2.0 changes:
  - DISTRO_GRID_ARCHIVE renamed to DISTRO_GRID_ARCHIVE_FULL.
  - DISTRO_GRID_MATCHED_ARCHIVE added as new filtered archive for Placement Intelligence.
  - DG_ARCHIVE_TRACKING updated with FULL_ARCHIVED_AT and MATCHED_ARCHIVED_AT columns,
    replacing the original ARCHIVED_AT column.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional, Literal
from datetime import date as _date


def infer_season_label(today: _date | None = None) -> str:
    """
    Infer a season label like 'Spring 2025' or 'Fall 2025' based on today's date.

    Rules (keep simple and predictable):
    - Months 1-6  -> 'Spring <year>'
    - Months 7-12 -> 'Fall <year>'

    This is used when archiving into DG_ARCHIVE_TRACKING so that
    you don't need a manual season picker in the UI.
    """
    d = today or _date.today()
    if 1 <= d.month <= 6:
        core = "Spring"
    else:
        core = "Fall"
    return f"{core} {d.year}"


@dataclass(frozen=True)
class ColumnSpec:
    """
    Declarative column spec for Distro Grid uploads.

    Attributes:
        name: Canonical column name.
        required_upload: Must be present in the formatted upload DataFrame.
        logical_type: Coarse logical type ("str", "int", "date", ...).
        allow_null: If False, NULLs in this column are validation errors.
        description: Short explanation for future devs.
    """
    name: str
    required_upload: bool = True
    logical_type: str = "str"
    allow_null: bool = False
    description: Optional[str] = None


# ---------------------------------------------------------------------
# Logical upload schema
# ---------------------------------------------------------------------

UPLOAD_COLUMNS: Dict[str, ColumnSpec] = {
    # NOTE:
    # - CHAIN_NAME is included in the upload file but the app will overwrite it
    #   using the user-selected chain before upload.
    "CHAIN_NAME": ColumnSpec(
        name="CHAIN_NAME",
        required_upload=True,
        logical_type="str",
        allow_null=False,
        description="Client chain name; UI selection wins if file disagrees.",
    ),
    "STORE_NAME": ColumnSpec(
        name="STORE_NAME",
        required_upload=True,
        logical_type="str",
        allow_null=False,
        description="Store display name as provided by client.",
    ),
    "STORE_NUMBER": ColumnSpec(
        name="STORE_NUMBER",
        required_upload=True,
        logical_type="int",
        allow_null=False,
        description="Numeric store identifier; cleaned of '#', etc.",
    ),
    "COUNTY": ColumnSpec(
        name="COUNTY",
        required_upload=True,
        logical_type="str",
        allow_null=False,
        description="County for the store.",
    ),
    "UPC": ColumnSpec(
        name="UPC",
        required_upload=True,
        logical_type="str",
        allow_null=False,
        description="Item UPC (as string, supports 11/12/13-digit logic).",
    ),
    "SKU": ColumnSpec(
        name="SKU",
        required_upload=False,
        logical_type="int",
        allow_null=True,
        description="Internal item code if provided.",
    ),
    "PRODUCT_NAME": ColumnSpec(
        name="PRODUCT_NAME",
        required_upload=True,
        logical_type="str",
        allow_null=False,
        description="Product name; should align with PRODUCTS.PRODUCT_NAME.",
    ),
    "MANUFACTURER": ColumnSpec(
        name="MANUFACTURER",
        required_upload=False,
        logical_type="str",
        allow_null=True,
        description="Supplier/manufacturer, maps to PRODUCTS.SUPPLIER.",
    ),
    "SEGMENT": ColumnSpec(
        name="SEGMENT",
        required_upload=False,
        logical_type="str",
        allow_null=True,
        description="Segment/category for reporting.",
    ),
    "YES_NO": ColumnSpec(
        name="YES_NO",
        required_upload=True,
        logical_type="int",
        allow_null=False,
        description="Placement flag: 1 = yes/in schematic, 0 = no.",
    ),
    "ACTIVATION_STATUS": ColumnSpec(
        name="ACTIVATION_STATUS",
        required_upload=False,
        logical_type="str",
        allow_null=True,
        description="Optional status label (e.g. ACTIVE/INACTIVE).",
    ),
    # Injected/derived fields, not required from upload:
    "TENANT_ID": ColumnSpec(
        name="TENANT_ID",
        required_upload=False,
        logical_type="str",
        allow_null=False,
        description="Tenant identifier (injected before upload).",
    ),
    "CUSTOMER_ID": ColumnSpec(
        name="CUSTOMER_ID",
        required_upload=False,
        logical_type="int",
        allow_null=True,
        description="FK to CUSTOMERS, filled by UPDATE_DISTRO_GRID().",
    ),
    "PRODUCT_ID": ColumnSpec(
        name="PRODUCT_ID",
        required_upload=False,
        logical_type="int",
        allow_null=True,
        description="FK to PRODUCTS, filled by UPDATE_DISTRO_GRID().",
    ),
}


VALID_YES_NO_VALUES = {0, 1}
VALID_ACTIVATION_STATUS = {"ACTIVE", "INACTIVE"}


# ---------------------------------------------------------------------
# Physical table shapes (for reference)
# ---------------------------------------------------------------------

DISTRO_GRID_DB_COLUMNS: List[str] = [
    "DISTRO_GRID_ID",
    "TENANT_ID",
    "CUSTOMER_ID",
    "CHAIN_NAME",
    "STORE_NAME",
    "STORE_NUMBER",
    "COUNTY",
    "PRODUCT_ID",
    "UPC",
    "SKU",
    "PRODUCT_NAME",
    "MANUFACTURER",
    "SEGMENT",
    "YES_NO",
    "ACTIVATION_STATUS",
    "CREATED_AT",
    "UPDATED_AT",
    "LAST_LOAD_DATE",
]

# v1.2.0: Renamed from DISTRO_GRID_ARCHIVE_DB_COLUMNS.
# DISTRO_GRID_ARCHIVE_FULL is the raw unfiltered backup of every chain grid row —
# all UPCs matched or not. Used for data recovery only. Retention: 1 year rolling.
DISTRO_GRID_ARCHIVE_FULL_DB_COLUMNS: List[str] = [
    "DISTRO_GRID_ARCHIVE_ID",
    "TENANT_ID",
    "CUSTOMER_ID",
    "CHAIN_NAME",
    "STORE_NAME",
    "STORE_NUMBER",
    "COUNTY",
    "PRODUCT_ID",
    "UPC",
    "SKU",
    "PRODUCT_NAME",
    "MANUFACTURER",
    "SEGMENT",
    "YES_NO",
    "ACTIVATION_STATUS",
    "CREATED_AT",
    "UPDATED_AT",
    "LAST_LOAD_DATE",
    "ARCHIVE_DATE",
]

# v1.2.0: New table. DISTRO_GRID_MATCHED_ARCHIVE contains only Delta Pacific
# placements that pass the three-way filter:
#   1. PRODUCT_ID <> 0    — product exists in Delta Pacific catalog
#   2. COUNTY is valid    — store is in a served territory
#   3. SUPPLIER_COUNTY    — manufacturer is authorized for that county
# Used exclusively by Placement Intelligence for season-over-season comparisons.
# Retention: 2 years rolling.
DISTRO_GRID_MATCHED_ARCHIVE_DB_COLUMNS: List[str] = [
    "DISTRO_GRID_ARCHIVE_ID",
    "TENANT_ID",
    "CUSTOMER_ID",
    "CHAIN_NAME",
    "STORE_NAME",
    "STORE_NUMBER",
    "COUNTY",
    "PRODUCT_ID",
    "UPC",
    "SKU",
    "PRODUCT_NAME",
    "MANUFACTURER",
    "SEGMENT",
    "YES_NO",
    "ACTIVATION_STATUS",
    "CREATED_AT",
    "UPDATED_AT",
    "LAST_LOAD_DATE",
    "ARCHIVE_DATE",
]

# v1.2.0: ARCHIVED_AT column replaced by FULL_ARCHIVED_AT and MATCHED_ARCHIVED_AT
# to track which archive tables have been written for each chain+season.
# Both timestamps are stamped together in the same transaction at upload time.
DG_ARCHIVE_TRACKING_DB_COLUMNS: List[str] = [
    "CHAIN_NAME",
    "SEASON",
    "ARCHIVED_AT",          # legacy — kept for backward compatibility, no longer written
    "FULL_ARCHIVED_AT",     # stamped when DISTRO_GRID_ARCHIVE_FULL is written
    "MATCHED_ARCHIVED_AT",  # stamped when DISTRO_GRID_MATCHED_ARCHIVE is written
]

RESET_SCHEDULE_DB_COLUMNS: List[str] = [
    "RESET_SCHEDULE_ID",
    "CHAIN_NAME",
    "STORE_NUMBER",
    "STORE_NAME",
    "PHONE_NUMBER",
    "CITY",
    "ADDRESS",
    "STATE",
    "COUNTY",
    "TEAM_LEAD",
    "RESET_DATE",
    "RESET_TIME",
    "STATUS",
    "NOTES",
    "TENANT_ID",
    "CREATED_AT",
    "UPDATED_AT",
    "LAST_LOAD_DATE",
]