# -*- coding: utf-8 -*-
"""
utils/pdf_reports.py

Page Overview
-------------
Builds a branded "Predictive Purchases" PDF using reportlab.
- Header band uses Chainlink primary color (#6497D6).
- Title + metadata (tenant, horizon).
- Compact table: UPC, PRODUCT_ID, Forecast Units, Forecast Revenue.
- Automatic pagination with ~36 data rows per page (tuned for LETTER).
- ASCII sanitization to avoid Windows-1252 "smart" characters issues.

Dev Notes
---------
- Keep this UTF-8. If you must use typographic dashes, prefer explicit escapes (e.g., "\u2013")
  and a font that supports them. This module sticks to ASCII-safe output by default.
- If reportlab isn't installed, the function returns a small text blob so exports still work.
"""

from __future__ import annotations
from reportlab.lib.pagesizes import LETTER, landscape  


from io import BytesIO
from typing import Optional

# Soft dependency: reportlab. Fall back gracefully if missing.
try:
    from reportlab.lib.pagesizes import LETTER
    from reportlab.pdfgen import canvas
    from reportlab.lib import colors
    from reportlab.lib.units import inch
    _HAS_REPORTLAB = True
except Exception:  # pragma: no cover
    _HAS_REPORTLAB = False

# ---- Chainlink Palette (keep in sync with app theme) ----
PRIMARY_HEX = "#6497D6"  # Primary
NEUTRAL_BG_HEX = "#F8F2EB"  # Background/neutral

# Lazy colors import fallback
if _HAS_REPORTLAB:
    PRIMARY = colors.HexColor(PRIMARY_HEX)
    NEUTRAL_BG = colors.HexColor(NEUTRAL_BG_HEX)


# ---------------------------
# Public API
# ---------------------------

def build_predictive_purchases_pdf(
    tenant_name: str,
    horizon_weeks: int,
    summary_table,
    *,
    title: str = "Chainlink - Predictive Purchases",
    rows_per_page: int = 36,
) -> bytes:
    """
    Render a lightweight, branded PDF with header band, title, and a table of recommendations.

    Args:
        tenant_name: Tenant display name (shown in header).
        horizon_weeks: Forecast horizon (e.g., 4).
        summary_table: pandas.DataFrame with columns:
            ["UPC", "PRODUCT_ID", "Forecast_Units_Next_Period", "Forecast_Revenue_Next_Period"]
        title: Optional document title shown in the header band.
        rows_per_page: Max table rows per page (excluding header row).

    Returns:
        bytes: PDF file bytes suitable for st.download_button.
    """
    if not _HAS_REPORTLAB:
        # Graceful fallback so users can still download *something*
        text = (
            f"{title}\n"
            f"Tenant: {tenant_name}\n"
            f"Horizon: {horizon_weeks} week(s)\n\n"
            "Install 'reportlab' to enable full PDF rendering."
        )
        return text.encode("utf-8")

    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=LETTER)
    page_w, page_h = LETTER

    # --- First page header band ---
    _draw_header_band(c, page_w, page_h, title, tenant_name, horizon_weeks)

    # --- Title line ---
    y = page_h - 1.15 * inch
    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(_x_left(), y, "Recommendations (tenant-wide, by UPC + Product ID)")
    y -= 0.24 * inch

    # --- Table header ---
    headers = ["UPC", "PRODUCT_ID", "Forecast Units (next period)", "Forecast Revenue"]
    col_widths = [1.8 * inch, 1.8 * inch, 2.1 * inch, 1.8 * inch]
    y = _draw_table_header(c, y, headers, col_widths)

    # --- Table rows (paginated) ---
    if summary_table is not None and len(summary_table) > 0:
        # Iterate rows with pagination
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
                _draw_header_band(c, page_w, page_h, f"{title} (continued)", tenant_name, horizon_weeks)
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


# ---------------------------
# Internal helpers
# ---------------------------

def _x_left() -> float:
    return 0.5 * inch


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
    """Render a light, unobtrusive footer."""
    c.setFont("Helvetica", 8)
    c.setFillColor(colors.grey)
    c.drawRightString(
        page_w - _x_left(),
        0.5 * inch,
        _ascii_safe("Generated by Chainlink Core • © 2025 Chainlink Analytics LLC"),
    )


def _ascii_safe(val) -> str:
    """
    Convert arbitrary value to a simple ASCII-safe string.
    Replaces common Windows-1252 glyphs with plain equivalents.
    """
    s = str(val if val is not None else "")
    # Basic replacements (extend as needed)
    replacements = {
        "\u2013": "-",  # en dash
        "\u2014": "-",  # em dash
        "\u2018": "'",  # left single quote
        "\u2019": "'",  # right single quote
        "\u201C": '"',  # left double quote
        "\u201D": '"',  # right double quote
        "\u2026": "...",  # ellipsis
        "\xa0": " ",     # non-breaking space
    }
    for k, v in replacements.items():
        s = s.replace(k, v)
    # Drop any remaining non-ASCII chars
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



def build_predictive_truck_pdf(
    week_start,
    horizon_weeks,
    summary_df,
    detail_df,
    *,
    tenant_name: str | None = None,
    tenant_id: str | None = None,
    run_id: str | None = None,
) -> bytes:
    """
    Predictive Truck Plan PDF (branded)
    - Cover: tenant, week_start (Monday), horizon, run_id
    - Summary table: salesperson totals
    - Detail: per-salesperson tables with
      Store #, Chain, Store, UPC, Product Name, Pred (lo–hi).
    """
    effective_tenant = tenant_name or tenant_id or "N/A"

    # Soft import so module import never crashes
    try:
        from reportlab.lib import colors
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.platypus import (
            SimpleDocTemplate,
            Paragraph,
            Spacer,
            Table,
            TableStyle,
            PageBreak,
        )
        HAS_RL = True
    except Exception:
        HAS_RL = False

    # Fallback if reportlab missing
    if not HAS_RL:
        txt = (
            "Predictive Truck Plan\n"
            f"Tenant: {effective_tenant}\n"
            f"Week start (Mon): {week_start}\n"
            f"Horizon (weeks): {horizon_weeks}\n"
            f"RUN_ID: {run_id or 'preview'}\n"
            "Install 'reportlab' for full PDF rendering."
        )
        return txt.encode("utf-8")

    import io
    buf = io.BytesIO()

    # Colors: reuse Chainlink palette if present; fall back to hex literals
    try:
        primary = PRIMARY  # from module-level palette if defined above
    except NameError:
        from reportlab.lib import colors as _c
        primary = _c.HexColor("#6497D6")

    # 👉 LANDSCAPE LETTER
    doc = SimpleDocTemplate(
        buf,
        pagesize=landscape(LETTER),
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

    # ------------------------------------------------------------------ #
    # Helper: dataframe → ReportLab table
    # ------------------------------------------------------------------ #
    def _df_to_table(df, col_widths=None, numeric_cols=None):
        # Convert product-name-like columns to Paragraph for wrapping
        from reportlab.platypus import Paragraph

        data = [list(df.columns)]
        for _, row in df.iterrows():
            row_cells = []
            for col, val in row.items():
                if isinstance(val, (int, float)) and numeric_cols and col in numeric_cols:
                    row_cells.append(val)
                elif col in ("Product Name",):
                    # Wrap long product names
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
        # Right-align numeric prediction columns
        if numeric_cols:
            for idx, col in enumerate(df.columns):
                if col in numeric_cols:
                    t.setStyle(
                        TableStyle(
                            [
                                ("ALIGN", (idx, 1), (idx, -1), "RIGHT"),
                            ]
                        )
                    )
        return t

    story = []

    # ------------------------------------------------------------------ #
    # Cover
    # ------------------------------------------------------------------ #
    story.append(Paragraph("Predictive Truck Plan", h1))
    for line in [
        f"Tenant: <b>{effective_tenant}</b>",
        f"Week start (Monday): <b>{week_start}</b>",
        f"Horizon (weeks): <b>{horizon_weeks}</b>",
        f"Run ID: <b>{run_id or 'preview'}</b>",
    ]:
        story.append(Paragraph(line, body))
    story.append(Spacer(1, 12))

    # ------------------------------------------------------------------ #
    # Summary
    # ------------------------------------------------------------------ #
    story.append(Paragraph("Summary by Salesperson", h2))
    if summary_df is None or summary_df.empty:
        story.append(Paragraph("No summary rows.", body))
    else:
        summary_view = (
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
        summary_tbl = _df_to_table(
            summary_view,
            col_widths=[160, 100, 80, 60],
            numeric_cols=["Total Cases", "Stores", "SKUs"],
        )
        story.append(summary_tbl)
    story.append(Spacer(1, 18))

    # ------------------------------------------------------------------ #
    # Detail (Store → UPC)
    # ------------------------------------------------------------------ #
    story.append(Paragraph("Detail (Store → UPC)", h2))
    if detail_df is None or detail_df.empty:
        story.append(Paragraph("No detail rows to display.", body))
    else:
        df = detail_df.copy()

        # Ensure numeric prediction columns are rounded nicely
        df["PRED_CASES"] = df["PRED_CASES"].astype(float).round(2)
        df["PRED_CASES_LO"] = df["PRED_CASES_LO"].astype(float).round(2)
        df["PRED_CASES_HI"] = df["PRED_CASES_HI"].astype(float).round(2)

        # Sorting to keep the PDF readable
        sort_cols = [
            "SALESPERSON",
            "CHAIN_NAME",
            "STORE_NAME",
            "STORE_NUMBER",
            "UPC",
        ]
        existing_sort_cols = [c for c in sort_cols if c in df.columns]
        if existing_sort_cols:
            df = df.sort_values(existing_sort_cols)

        # Group by salesperson so each driver has their own section
        for sp, g in df.groupby("SALESPERSON", dropna=False):
            story.append(Paragraph(f"Salesperson: {sp}", h3))
            story.append(
                Paragraph(
                    "Columns: Store #, Chain, Store, UPC, Product Name, Pred (lo–hi)",
                    small,
                )
            )
            story.append(Spacer(1, 6))

            # Ensure PRODUCT_NAME exists, even if empty
            if "PRODUCT_NAME" not in g.columns:
                g = g.copy()
                g["PRODUCT_NAME"] = ""

            # Match the CSV column order:
            # Store #, Chain, Store, UPC, Product Name, Pred, Lo, Hi
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

            # Paginate rows into manageable chunks
            rows_per_table = 30
            for i in range(0, len(view), rows_per_table):
                chunk = view.iloc[i : i + rows_per_table]

                story.append(
                    _df_to_table(
                        chunk,
                        # 8 columns: Store #, Chain, Store, UPC, Product Name, Pred, Lo, Hi
                        # tuned for LANDSCAPE LETTER
                        col_widths=[50, 70, 90, 80, 220, 55, 45, 45],
                        numeric_cols=["Pred", "Lo", "Hi"],
                    )
                )
                story.append(Spacer(1, 10))

            story.append(PageBreak())

    # ------------------------------------------------------------------ #
    # Footer branding on each page
    # ------------------------------------------------------------------ #
    def _on_page(canvas, doc):
        canvas.saveState()
        canvas.setFillColor(primary)
        canvas.rect(36, 36, doc.width, 2, stroke=0, fill=1)
        canvas.setFillColor(colors.grey)
        canvas.setFont("Helvetica", 8)
        canvas.drawRightString(
            doc.width + 36,
            24,
            "Generated by Chainlink Core • © 2025 Chainlink Analytics LLC",
        )
        canvas.restoreState()

    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)
    pdf = buf.getvalue()
    buf.close()
    return pdf
