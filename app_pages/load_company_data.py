# --- pages/load_company_data.py ---

import streamlit as st
from app_pages.load_company_sections import (
    render_sales_section,
    render_customers_section,
    render_products_section,
    render_supplier_county_section
)


# def render():
#     st.title("Format and Upload Company Data")

#     render_sales_section()
#     st.markdown("<hr>", unsafe_allow_html=True)
#     render_customers_section()
#     st.markdown("<hr>", unsafe_allow_html=True)
#     render_products_section()
#     st.markdown("<hr>", unsafe_allow_html=True)
#     render_supplier_county_section()


def render():
    st.title("Format and Upload Company Data")

    with st.expander("Sales Report", expanded=False):
        render_sales_section()

    with st.expander("Customers", expanded=False):
        render_customers_section()

    with st.expander("Products", expanded=False):
        render_products_section()

    with st.expander("Supplier by County", expanded=False):
        render_supplier_county_section()
