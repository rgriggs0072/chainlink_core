# -*- coding: utf-8 -*-
"""
utils/pdf_reports.py

PDF Report Builders for Chainlink Core
--------------------------------------

Overview:
- Central place for all PDF generators used in Chainlink Core.

Public functions:
- build_predictive_purchases_pdf(...)
    -> Predictive Purchases summary (tenant-wide, by UPC + PRODUCT_ID)

- build_predictive_truck_pdf(...)
    -> Predictive Truck Plan (landscape, per-salesperson pages)

- build_gap_streaks_pdf(...)
    -> Gap streaks by salesperson/store/item, with streak-based coloring
       and a narrow STREAK_WEEKS column so the table fits nicely.

Notes:
- Uses ReportLab when available. If ReportLab is missing, each builder
  returns a simple text payload instead of crashing the app.
- Color palette is aligned with the Chainlink theme:
    PRIMARY_HEX      = "#6497D6"
    NEUTRAL_BG_HEX   = "#F8F2EB"
"""

from __future__ import annotations

from datetime import datetime
from io import BytesIO
from typing import Optional

import pandas as pd

# -------------------------------------------------------------------
# Soft dependency: ReportLab
# -------------------------------------------------------------------
try:
    from reportlab.lib.pagesizes import letter, landscape
    from reportlab.pdfgen import canvas
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.platypus import (
        SimpleDocTemplate,
        Table,
        TableStyle,
        Paragraph,
        Spacer,
        PageBreak,
    )
    from reportlab.lib.enums import TA_LEFT, TA_CENTER

    _HAS_REPORTLAB = True
except Exception:  # pragma: no cover
    # Fallback stubs so imports don't explode if ReportLab isn't installed.
    _HAS_REPORTLAB = False
    letter = landscape = None
    colors = None
    inch = 72  # arbitrary default
    canvas = None
    SimpleDocTemplate = Table = TableStyle = Paragraph = Spacer = PageBreak = None
    getSampleStyleSheet = ParagraphStyle = None

# -------------------------------------------------------------------
# Chainlink Palette (keep in sync with app theme)
# -------------------------------------------------------------------
PRIMARY_HEX = "#6497D6"  # Primary
NEUTRAL_BG_HEX = "#F8F2EB"  # Background / neutral

if _HAS_REPORTLAB:
    PRIMARY = colors.HexColor(PRIMARY_HEX)
    NEUTRAL_BG = colors.HexColor(NEUTRAL_BG_HEX)


# ===================================================================
# Helper: ASCII-safe text and numeric formatting
# ===================================================================

def _ascii_safe(val) -> str:
    """
    Convert arbitrary value to a simple ASCII-safe string.
    Replaces common Windows-1252 glyphs with plain equivalents.
    """
    s = str(val if val is not None else "")
    replacements = {
        "\u2013": "-",   # en dash
        "\u2014": "-",   # em dash
        "\u2018": "'",   # left single quote
        "\u2019": "'",   # right single quote
        "\u201C": '"',   # left double quote
        "\u201D": '"',   # right double quote
        "\u2026": "...", # ellipsis
        "\xa0": " ",     # non-breaking space
    }
    for k, v in replacements.items():
        s = s.replace(k, v)

    try:
        s.encode("ascii", "strict")
        return s
    except UnicodeEncodeError:
        return s.encode("ascii", "ignore").decode("ascii")


def _fmt_num(x) -> str:
    """Format numeric value with two decimals."""
    try:
        return f"{float(x):,.2f}"
    except Exception:
        return "0.00"


def _fmt_currency(x) -> str:
    """Format currency with dollar sign and two decimals."""
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return "$0.00"


def _x_left() -> float:
    """Left margin helper for canvas-based PDFs."""
    return 0.5 * inch



# ===================================================================
# Shared constants (keep column contracts centralized)
# ===================================================================

# NOTE:
# - This column list is the canonical "Gap History PDF input contract".
# - Any page/email code that builds the "gap streaks" PDF should slice using this list.
# - build_gap_streaks_pdf() will still defensively create ADDRESS if missing,
#   but upstream should always try to supply it.
GAP_HISTORY_PDF_COLUMNS = [
    "CHAIN_NAME",
    "STORE_NUMBER",
    "STORE_NAME",
    "ADDRESS",
    "SUPPLIER_NAME",
    "PRODUCT_NAME",
    "UPC",
    "STREAK_WEEKS",
    "FIRST_GAP_WEEK",
    "LAST_GAP_WEEK",
]



# ===================================================================
# Predictive Purchases PDF
# ===================================================================

def build_predictive_purchases_pdf(
    tenant_name: str,
    horizon_weeks: int,
    summary_table: pd.DataFrame,
    *,
    title: str = "Chainlink - Predictive Purchases",
    rows_per_page: int = 36,
) -> bytes:
    """
    Render a lightweight, branded PDF with header band, title, and a table
    of predictive purchase recommendations.

    Args:
        tenant_name: Tenant display name (shown in header).
        horizon_weeks: Forecast horizon (e.g., 4).
        summary_table: DataFrame with columns:
            ["UPC", "PRODUCT_ID", "Forecast_Units_Next_Period", "Forecast_Revenue_Next_Period"]
        title: Document title for the header band.
        rows_per_page: Max table rows per page (excluding header row).

    Returns:
        bytes: PDF bytes (suitable for st.download_button).
    """
    if not _HAS_REPORTLAB:
        text = (
            f"{title}\n"
            f"Tenant: {tenant_name}\n"
            f"Horizon: {horizon_weeks} week(s)\n\n"
            "Install 'reportlab' to enable full PDF rendering."
        )
        return text.encode("utf-8")

    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=letter)
    page_w, page_h = letter

    # --- Header band ---
    _draw_header_band(c, page_w, page_h, title, tenant_name, horizon_weeks)

    # --- Section title ---
    y = page_h - 1.15 * inch
    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(
        _x_left(),
        y,
        "Recommendations (tenant-wide, by UPC + Product ID)",
    )
    y -= 0.24 * inch

    # --- Table header ---
    headers = ["UPC", "PRODUCT_ID", "Forecast Units (next period)", "Forecast Revenue"]
    col_widths = [1.8 * inch, 1.8 * inch, 2.1 * inch, 1.8 * inch]
    y = _draw_table_header(c, y, headers, col_widths)

    # --- Table rows with pagination ---
    if summary_table is not None and len(summary_table) > 0:
        rows_on_page = 0
        c.setFont("Helvetica", 10)

        for _, row in summary_table.iterrows():
            vals = [
                _ascii_safe(row.get("UPC", "")),
                _ascii_safe(row.get("PRODUCT_ID", "")),
                _fmt_num(row.get("Forecast_Units_Next_Period", 0.0)),
                _fmt_currency(row.get("Forecast_Revenue_Next_Period", 0.0)),
            ]

            # New page if needed
            if rows_on_page >= rows_per_page or y < 1.0 * inch:
                c.showPage()
                _draw_header_band(
                    c,
                    page_w,
                    page_h,
                    f"{title} (continued)",
                    tenant_name,
                    horizon_weeks,
                )
                y = page_h - 0.95 * inch
                y = _draw_table_header(c, y, headers, col_widths)
                c.setFont("Helvetica", 10)
                rows_on_page = 0

            # Draw row
            x = _x_left()
            for i, val in enumerate(vals):
                c.drawString(x, y, val)
                x += col_widths[i]
            y -= 0.18 * inch
            rows_on_page += 1
    else:
        c.setFont("Helvetica-Oblique", 10)
        c.drawString(_x_left(), y, "No recommendations available.")
        y -= 0.18 * inch

    # --- Footer ---
    _draw_footer(c, page_w)

    c.save()
    buf.seek(0)
    return buf.getvalue()


def _draw_header_band(c, page_w: float, page_h: float, title: str, tenant_name: str, horizon_weeks: int) -> None:
    """Render the top header band with title and metadata."""
    c.setFillColor(PRIMARY)
    c.rect(0, page_h - 0.8 * inch, page_w, 0.8 * inch, stroke=0, fill=1)

    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(_x_left(), page_h - 0.5 * inch, _ascii_safe(title))

    c.setFont("Helvetica", 10)
    right_text = f"Tenant: {tenant_name} | Horizon: {horizon_weeks}w"
    c.drawRightString(page_w - _x_left(), page_h - 0.5 * inch, _ascii_safe(right_text))


def _draw_table_header(c, y: float, headers: list[str], col_widths: list[float]) -> float:
    """Draw table header row and underline; return next y."""
    c.setFont("Helvetica-Bold", 10)
    x = _x_left()
    for i, col in enumerate(headers):
        c.drawString(x, y, _ascii_safe(col))
        x += col_widths[i]
    y -= 0.15 * inch
    c.line(_x_left(), y, _x_left() + sum(col_widths), y)
    return y - 0.10 * inch


def _draw_footer(c, page_w: float) -> None:
    """Render a light, unobtrusive footer for canvas-based PDFs."""
    c.setFont("Helvetica", 8)
    c.setFillColor(colors.grey)
    c.drawRightString(
        page_w - _x_left(),
        0.5 * inch,
        _ascii_safe("Generated by Chainlink Core • © 2025 Chainlink Analytics LLC"),
    )


# ===================================================================
# Predictive Truck Plan PDF
# ===================================================================

def build_predictive_truck_pdf(
    week_start,
    horizon_weeks,
    summary_df: pd.DataFrame | None,
    detail_df: pd.DataFrame | None,
    *,
    tenant_name: str | None = None,
    tenant_id: str | None = None,
    run_id: str | None = None,
) -> bytes:
    """
    Predictive Truck Plan PDF (branded, landscape).

    - Cover: tenant, week_start (Monday), horizon, run_id
    - Summary table: salesperson totals
    - Detail: per-salesperson tables with:
        Store #, Chain, Store, UPC, Product Name, Pred (lo–hi).
    """
    effective_tenant = tenant_name or tenant_id or "N/A"

    if not _HAS_REPORTLAB:
        txt = (
            "Predictive Truck Plan\n"
            f"Tenant: {effective_tenant}\n"
            f"Week start (Mon): {week_start}\n"
            f"Horizon (weeks): {horizon_weeks}\n"
            f"RUN_ID: {run_id or 'preview'}\n"
            "Install 'reportlab' for full PDF rendering."
        )
        return txt.encode("utf-8")

    buf = BytesIO()

    # Colors: reuse Chainlink palette if present
    primary = PRIMARY if _HAS_REPORTLAB else colors.HexColor("#6497D6")

    # LANDSCAPE LETTER
    doc = SimpleDocTemplate(
        buf,
        pagesize=landscape(letter),
        leftMargin=36,
        rightMargin=36,
        topMargin=36,
        bottomMargin=36,
    )
    styles = getSampleStyleSheet()
    h1, h2, h3, body = (

        styles["Heading1"],
        styles["Heading2"],
        styles["Heading3"],
        styles["BodyText"],
    )
    small = ParagraphStyle("Small", parent=body, fontSize=9, leading=11)

    # Helper: dataframe → ReportLab table
    def _df_to_table(df: pd.DataFrame, col_widths=None, numeric_cols=None):
        data = [list(df.columns)]
        for _, row in df.iterrows():
            row_cells = []
            for col, val in row.items():
                if isinstance(val, (int, float)) and numeric_cols and col in numeric_cols:
                    row_cells.append(val)
                elif col in ("Product Name",):
                    row_cells.append(Paragraph(str(val or ""), small))
                else:
                    row_cells.append(str(val) if val is not None else "")
            data.append(row_cells)

        t = Table(data, colWidths=col_widths) if col_widths else Table(data)
        t.setStyle(
            TableStyle(
                [
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                    ("FONTSIZE", (0, 0), (-1, -1), 8),
                    ("ALIGN", (0, 0), (-1, 0), "CENTER"),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F0F0F0")),
                    (
                        "ROWBACKGROUNDS",
                        (0, 1),
                        (-1, -1),
                        [colors.white, colors.HexColor("#FBFBFB")],
                    ),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#DDDDDD")),
                    ("LEFTPADDING", (0, 0), (-1, -1), 3),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 3),
                    ("TOPPADDING", (0, 0), (-1, -1), 2),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                ]
            )
        )
        if numeric_cols:
            for idx, col in enumerate(df.columns):
                if col in numeric_cols:
                    t.setStyle(
                        TableStyle(
                            [("ALIGN", (idx, 1), (idx, -1), "RIGHT")]
                        )
                    )
        return t

    story = []

    # Cover
    story.append(Paragraph("Predictive Truck Plan", h1))
    for line in [
        f"Tenant: <b>{effective_tenant}</b>",
        f"Week start (Monday): <b>{week_start}</b>",
        f"Horizon (weeks): <b>{horizon_weeks}</b>",
        f"Run ID: <b>{run_id or 'preview'}</b>",
    ]:
        story.append(Paragraph(line, body))
    story.append(Spacer(1, 12))

    # Summary
    story.append(Paragraph("Summary by Salesperson", h2))
    if summary_df is None or summary_df.empty:
        story.append(Paragraph("No summary rows.", body))
    else:
        view = (
            summary_df[["SALESPERSON", "TOTAL_CASES", "STORES", "SKUS"]]
            .rename(
                columns={
                    "SALESPERSON": "Salesperson",
                    "TOTAL_CASES": "Total Cases",
                    "STORES": "Stores",
                    "SKUS": "SKUs",
                }
            )
        )
        tbl = _df_to_table(
            view,
            col_widths=[160, 100, 80, 60],
            numeric_cols=["Total Cases", "Stores", "SKUs"],
        )
        story.append(tbl)
    story.append(Spacer(1, 18))

    # Detail
    story.append(Paragraph("Detail (Store → UPC)", h2))
    if detail_df is None or detail_df.empty:
        story.append(Paragraph("No detail rows to display.", body))
    else:
        df = detail_df.copy()
        df["PRED_CASES"] = df["PRED_CASES"].astype(float).round(2)
        df["PRED_CASES_LO"] = df["PRED_CASES_LO"].astype(float).round(2)
        df["PRED_CASES_HI"] = df["PRED_CASES_HI"].astype(float).round(2)

        sort_cols = [
            "SALESPERSON",
            "CHAIN_NAME",
            "STORE_NAME",
            "STORE_NUMBER",
            "UPC",
        ]
        existing = [c for c in sort_cols if c in df.columns]
        if existing:
            df = df.sort_values(existing)

        for sp, g in df.groupby("SALESPERSON", dropna=False):
            story.append(Paragraph(f"Salesperson: {sp}", h3))
            story.append(
                Paragraph(
                    "Columns: Store #, Chain, Store, UPC, Product Name, Pred (lo–hi)",
                    small,
                )
            )
            story.append(Spacer(1, 6))

            if "PRODUCT_NAME" not in g.columns:
                g = g.copy()
                g["PRODUCT_NAME"] = ""

            view = g[
                [
                    "STORE_NUMBER",
                    "CHAIN_NAME",
                    "STORE_NAME",
                    "UPC",
                    "PRODUCT_NAME",
                    "PRED_CASES",
                    "PRED_CASES_LO",
                    "PRED_CASES_HI",
                ]
            ].rename(
                columns={
                    "STORE_NUMBER": "Store #",
                    "CHAIN_NAME": "Chain",
                    "STORE_NAME": "Store",
                    "UPC": "UPC",
                    "PRODUCT_NAME": "Product Name",
                    "PRED_CASES": "Pred",
                    "PRED_CASES_LO": "Lo",
                    "PRED_CASES_HI": "Hi",
                }
            )

            rows_per_table = 30
            for i in range(0, len(view), rows_per_table):
                chunk = view.iloc[i : i + rows_per_table]
                story.append(
                    _df_to_table(
                        chunk,
                        col_widths=[50, 70, 90, 80, 220, 55, 45, 45],

                        numeric_cols=["Pred", "Lo", "Hi"],
                    )
                )
                story.append(Spacer(1, 10))

            story.append(PageBreak())

    # Footer per page
    def _on_page(canvas_, doc_):
        canvas_.saveState()
        canvas_.setFillColor(primary)
        canvas_.rect(36, 36, doc_.width, 2, stroke=0, fill=1)
        canvas_.setFillColor(colors.grey)
        canvas_.setFont("Helvetica", 8)
        canvas_.drawRightString(
            doc_.width + 36,
            24,
            "Generated by Chainlink Core • © 2025 Chainlink Analytics LLC",
        )
        canvas_.restoreState()

    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)
    pdf = buf.getvalue()
    buf.close()
    return pdf


# ===================================================================
# Gap Streaks PDF (streak colors + narrow STREAK_WEEKS col)
# ===================================================================

def build_gap_streaks_pdf(
    df: pd.DataFrame,
    tenant_name: str = "Client",
    salesperson_name: Optional[str] = None,
    as_of_date: Optional[datetime] = None,
) -> bytes:
    """
    Build a Gap Streaks PDF with a compact landscape table.

    Table columns:
        Store#, Store, Address, Supplier, Product, Wks

    Header includes tenant + salesperson (so we do NOT repeat salesperson per row).

    Color coding by streak length:
        2 weeks  -> soft yellow
        3 weeks  -> soft orange
        4+ weeks -> soft red

    Dev Notes
    ---------
    - Uses Paragraph cells to wrap long text cleanly.
    - Sanitizes embedded newlines/tabs and collapses whitespace to prevent
      "stacked" / exploded rows in ReportLab tables.
    - Truncates long strings to keep the table readable.
    """
    if not _HAS_REPORTLAB:
        txt = f"Gap Streaks Report – {tenant_name}\nInstall 'reportlab' for full PDF rendering."
        return txt.encode("utf-8")

    if as_of_date is None:
        as_of_date = datetime.today()

    df_display = df.copy()

    # Normalize Address casing (some sources may return Address/address)
    if "ADDRESS" not in df_display.columns:
        for alt in ("Address", "address"):
            if alt in df_display.columns:
                df_display = df_display.rename(columns={alt: "ADDRESS"})
                break

    # -----------------------------
    # Ensure required columns exist
    # -----------------------------
    if "ADDRESS" not in df_display.columns:
        df_display["ADDRESS"] = ""

    # Make streak numeric + safe
    if "STREAK_WEEKS" in df_display.columns:
        df_display["STREAK_WEEKS"] = (
            pd.to_numeric(df_display["STREAK_WEEKS"], errors="coerce")
            .fillna(0)
            .astype(int)
        )
    else:
        df_display["STREAK_WEEKS"] = 0

    # -----------------------------
    # Sort: longest streaks first
    # -----------------------------
    sort_cols = [c for c in ["STREAK_WEEKS", "CHAIN_NAME", "STORE_NUMBER", "PRODUCT_NAME"] if c in df_display.columns]
    if sort_cols:
        asc = [False, True, True, True][: len(sort_cols)]
        df_display = df_display.sort_values(sort_cols, ascending=asc)

    # -----------------------------
    # Table columns (NO salesperson)
    # -----------------------------
    cols = [
        "STORE_NUMBER",
        "STORE_NAME",
        "ADDRESS",
        "SUPPLIER_NAME",
        "PRODUCT_NAME",
        "STREAK_WEEKS",
    ]
    cols = [c for c in cols if c in df_display.columns]

    header_labels_map = {
        "STORE_NUMBER": "Store#",
        "STORE_NAME": "Store",
        "ADDRESS": "Address",
        "SUPPLIER_NAME": "Supplier",
        "PRODUCT_NAME": "Product",
        "STREAK_WEEKS": "Wks",
    }

    # -----------------------------
    # PDF layout
    # -----------------------------
    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=landscape(letter),
        leftMargin=30,
        rightMargin=30,
        topMargin=40,
        bottomMargin=30,
    )

    styles = getSampleStyleSheet()

    # Subtitle + legend (avoid clipping + nicer read)
    sub_style = styles["Normal"].clone("gap_streaks_subtitle")
    sub_style.fontSize = 10
    sub_style.leading = 12

    legend_style = styles["Normal"].clone("gap_streaks_legend")
    legend_style.fontSize = 10
    legend_style.leading = 12

    # --- Table cell styles (must be defined BEFORE _cell) ---
    cell_style = ParagraphStyle(
        "gap_cell",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=7,
        leading=9,
        alignment=TA_LEFT,
        wordWrap="CJK",   # wraps long tokens better than default
        spaceBefore=0,
        spaceAfter=0,
    )
    cell_style_center = ParagraphStyle(
        "gap_cell_center",
        parent=cell_style,
        alignment=TA_CENTER,
    )
    header_style = ParagraphStyle(
        "gap_header",
        parent=styles["Normal"],
        fontName="Helvetica-Bold",
        fontSize=8,
        leading=10,
        alignment=TA_CENTER,
        textColor=colors.white,
        spaceBefore=0,
        spaceAfter=0,
    )

    def _clean_text(val: object) -> str:
        """Force single-line, collapsed whitespace, ASCII-safe."""
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return ""
        s = str(val)
        s = s.replace("\r\n", " ").replace("\n", " ").replace("\r", " ").replace("\t", " ")
        s = " ".join(s.split())
        return _ascii_safe(s)

    def _truncate(s: str, max_len: int) -> str:
        if max_len and len(s) > max_len:
            return s[: max_len - 1] + "…"
        return s

    def _cell(val: object, *, center: bool = False, max_len: Optional[int] = None) -> Paragraph:
        """Return a wrapped Paragraph cell."""
        txt = _clean_text(val)
        if max_len:
            txt = _truncate(txt, max_len)
        return Paragraph(txt, cell_style_center if center else cell_style)

    # Tuned truncation per column to avoid ugly wraps
    trunc = {
        "STORE_NUMBER": 10,
        "STORE_NAME": 28,
        "ADDRESS": 40,
        "SUPPLIER_NAME": 30,
        "PRODUCT_NAME": 60,
        "STREAK_WEEKS": 3,
    }

    # -----------------------------
    # Build story header
    # -----------------------------
    story: list = []

    title_text = f"Gap Streaks Report – {tenant_name}"
    if salesperson_name:
        title_text += f" – {salesperson_name}"

    title = Paragraph(_clean_text(title_text), styles["Title"])
    subtitle = Paragraph(
        f"As of {as_of_date.strftime('%Y-%m-%d')} &nbsp;&nbsp; (streaks by store / item)",
        sub_style,
    )
    legend = Paragraph(
        (
            "<b>Legend:</b> "
            "<font color='#FFF9C4'>Yellow</font> = 2 weeks · "
            "<font color='#FFE0B2'>Orange</font> = 3 weeks · "
            "<font color='#FFCCCC'>Red</font> = 4+ weeks"
        ),
        legend_style,
    )

    story.extend([title, Spacer(1, 8), subtitle, Spacer(1, 6), legend, Spacer(1, 14)])

    # -----------------------------
    # Build table data ONCE
    # -----------------------------
    data: list[list[object]] = []

    # Header row as Paragraphs so it aligns perfectly
    data.append([Paragraph(header_labels_map.get(c, c), header_style) for c in cols])

    for _, row in df_display[cols].iterrows():
        row_cells: list[Paragraph] = []
        for c in cols:
            is_center = c in {"STORE_NUMBER", "STREAK_WEEKS"}
            row_cells.append(_cell(row.get(c), center=is_center, max_len=trunc.get(c)))
        data.append(row_cells)

    # Column widths (landscape letter)
    # Store#, Store, Address, Supplier, Product, Wks
    col_widths = [38, 80, 185, 135, 260, 34][: len(cols)]

    table = Table(data, colWidths=col_widths, repeatRows=1)

    # -----------------------------
    # Table styling
    # -----------------------------
    style_commands: list[tuple] = [
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor(PRIMARY_HEX)),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),

        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),

        # This prevents the "stacked columns" look
        ("VALIGN", (0, 0), (-1, -1), "TOP"),

        # Padding tuned for dense tables
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 2),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
    ]

    # Row color by streak length
    if "STREAK_WEEKS" in df_display.columns:
        for row_idx, (_, r) in enumerate(df_display.iterrows(), start=1):
            streak = int(r.get("STREAK_WEEKS", 0) or 0)
            bg_color = None
            if streak >= 4:
                bg_color = colors.HexColor("#FFCCCC")
            elif streak == 3:
                bg_color = colors.HexColor("#FFE0B2")
            elif streak == 2:
                bg_color = colors.HexColor("#FFF9C4")
            if bg_color is not None:
                style_commands.append(("BACKGROUND", (0, row_idx), (-1, row_idx), bg_color))

    table.setStyle(TableStyle(style_commands))
    story.append(table)

    doc.build(story)
    pdf_bytes = buf.getvalue()
    buf.close()
    return pdf_bytes
