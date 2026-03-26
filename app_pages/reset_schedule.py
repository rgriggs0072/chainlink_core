# --------------- app_pages/reset_schedule.py ---------------

import streamlit as st
from app_pages.reset_schedule_sections import (
    render_reset_schedule_formatter_section,
    render_reset_schedule_uploader_section,
    render_reset_schedule_editor_section,
)


def render():
    st.title("Reset Schedule")

    with st.expander("📥 Step 1: Download Template & Format", expanded=True):
        render_reset_schedule_formatter_section()

    with st.expander("⬆️ Step 2: Upload to Database", expanded=False):
        render_reset_schedule_uploader_section()

    # Only shown to admins — render_reset_schedule_editor_section()
    # has its own is_admin guard inside, but we also gate the expander
    # so non-admins never see the section header at all
    if st.session_state.get("is_admin"):
        with st.expander("✏️ Edit Reset Schedule", expanded=False):
            render_reset_schedule_editor_section()
