# ---------------- app_pages/distro_grid.py ----------------

import streamlit as st
from app_pages.distro_grid_sections import (
    render_distro_grid_formatter_section,
    render_distro_grid_uploader_section
)

def render():
    st.title("Distro Grid Formatter & Uploader")

    # Section 1: Format the file
    render_distro_grid_formatter_section()

    st.markdown("---")

    # Section 2: Upload the file
    render_distro_grid_uploader_section()

    st.markdown("---")

