# ----------- navigation_utils.py -----------
# utils/navigation.py
import streamlit as st

def get_nav_pages(user_roles: list[str]) -> dict:
    pages = {
        "🏠 Home": "app_pages.home",
        "📊 Reports": {
            "📉 Gap Report": "app_pages.gap_report",
            "📋 Execution Summary": "app_pages.exec_report",  # placeholder if not implemented
        },
        "📁 Format & Upload": {
            "📄 Load Company Data": "app_pages.load_company_data",
            "🧾 Import Distributor": "app_pages.import_distributor_report",
            "📦 Upload Catalog": "app_pages.upload_product_catalog"
        },
    }

    if "admin" in user_roles:
        pages["⚙️ Admin"] = "app_pages.admin"

    return pages

