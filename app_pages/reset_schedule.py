import streamlit as st
import openpyxl
import pandas as pd
from utils.ui_helpers import download_workbook
from datetime import datetime
from app_pages.reset_schedule_sections import (
    render_reset_schedule_formatter_section,
    render_reset_schedule_uploader_section
)

def render():
    st.title("🗓️ Reset Schedule Formatter & Uploader")

    render_reset_schedule_formatter_section()
    st.markdown("<hr>", unsafe_allow_html=True)
    render_reset_schedule_uploader_section()



