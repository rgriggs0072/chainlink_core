# utils/pdf_utils.py

from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas
from io import BytesIO
import datetime


def generate_ai_report_pdf(client_name, store_name, ai_text):
    """
    Generates a PDF file containing the AI narrative report.

    Args:
        client_name (str): The name of the client (e.g. from TOML or CLIENTS table).
        store_name (str): The name of the store selected in the report.
        ai_text (str): The AI-generated narrative text.

    Returns:
        BytesIO: A buffer containing the PDF content ready for download.
    """
    buffer = BytesIO()
    c = canvas.Canvas(buffer, pagesize=LETTER)
    width, height = LETTER
    margin = 50
    max_width = width - 2 * margin  # Account for left and right margins
    char_limit = 95  # Approximate line width that fits in LETTER with default font

    y_position = height - margin

    # Header
    c.setFont("Helvetica-Bold", 16)
    c.drawString(margin, y_position, f"{client_name} - AI Narrative Report")
    y_position -= 25

    # Metadata
    c.setFont("Helvetica", 10)
    c.drawString(margin, y_position, f"Store: {store_name}")
    y_position -= 15
    c.drawString(margin, y_position, f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    y_position -= 25

    # Body
    c.setFont("Helvetica", 11)
    text_obj = c.beginText(margin, y_position)
    text_obj.setLeading(14)

    for line in ai_text.split("\n"):
        wrapped_lines = textwrap.wrap(line, width=char_limit)  # ⬅ wrap each line
        for wrap_line in wrapped_lines:
            if y_position <= margin:
                c.drawText(text_obj)
                c.showPage()
                text_obj = c.beginText(margin, height - margin)
                text_obj.setLeading(14)
                y_position = height - margin
            text_obj.textLine(wrap_line)
            y_position -= 14  # update Y position manually

    c.drawText(text_obj)
    c.save()
    buffer.seek(0)
    return buffer