# ------------- load_company_sections.py -------------

import streamlit as st
import openpyxl
import pandas as pd
from utils.load_company_data_helpers import (
    format_sales_report, write_salesreport_to_snowflake,
    format_customers_report, write_customers_to_snowflake,
    format_product_workbook, write_products_to_snowflake,
    format_supplier_by_county, write_supplier_by_county_to_snowflake,
    download_workbook
)

# ------------------- SALES -------------------
def render_sales_section():
    st.subheader("Format Sales Report")
    sales_file = st.file_uploader("Upload Sales Report Excel", type=["xlsx"], key="sales_upload")

    if sales_file:
        try:
            wb = openpyxl.load_workbook(sales_file)
            with st.spinner("Formatting Sales Report..."):
                formatted = format_sales_report(wb)
            if formatted:
                download_workbook(formatted, "Formatted_Sales_Report.xlsx")
        except Exception as e:
            st.error(f"Error formatting Sales Report: {e}")

    st.markdown("---")
    st.subheader("Upload Formatted Sales Report to Snowflake")
    sales_final = st.file_uploader("Upload formatted Sales Report", type=["xlsx"], key="sales_final_upload")

    if sales_final:
        try:
            df = pd.read_excel(sales_final, engine="openpyxl", sheet_name="SALES REPORT")
            st.dataframe(df.head())
            if st.button("Upload to Snowflake", key="upload_sales_btn"):
                write_salesreport_to_snowflake(df)
        except Exception as e:
            st.error(f"Error uploading Sales Report: {e}")


# ------------------- CUSTOMERS -------------------
def render_customers_section():
    st.subheader("Format Customers")
    customers_file = st.file_uploader("Upload Customers Excel", type=["xlsx"], key="customers_upload")

    if customers_file:
        try:
            wb = openpyxl.load_workbook(customers_file)
            with st.spinner("Formatting Customers..."):
                formatted = format_customers_report(wb)
            if formatted:
                download_workbook(formatted, "Formatted_Customers.xlsx")
        except Exception as e:
            st.error(f"Error formatting Customers: {e}")

    st.markdown("---")
    st.subheader("Upload Formatted Customers to Snowflake")
    customers_final = st.file_uploader("Upload formatted Customers", type=["xlsx"], key="customers_final_upload")

    if customers_final:
        try:
            df = pd.read_excel(customers_final, engine="openpyxl", sheet_name="Customers")
            st.dataframe(df.head())
            if st.button("Upload to Snowflake", key="upload_customers_btn"):
                write_customers_to_snowflake(df)
        except Exception as e:
            st.error(f"Error uploading Customers: {e}")


# ------------------- PRODUCTS -------------------
def render_products_section():
    st.subheader("Format Products")
    products_file = st.file_uploader("Upload Products Excel", type=["xlsx", "csv"], key="products_upload")

    if products_file:
        try:
            wb = openpyxl.load_workbook(products_file)
            with st.spinner("Formatting Products..."):
                formatted = format_product_workbook(wb)
            if formatted:
                download_workbook(formatted, "Formatted_Products.xlsx")
        except Exception as e:
            st.error(f"Error formatting Products: {e}")

    st.markdown("---")
    st.subheader("Upload Formatted Products to Snowflake")
    products_final = st.file_uploader("Upload formatted Products", type=["xlsx", "csv"], key="products_final_upload")

    if products_final:
        try:
            df = pd.read_excel(products_final, engine="openpyxl", sheet_name="Products")
            st.dataframe(df.head())
            if st.button("Upload to Snowflake", key="upload_products_btn"):
                write_products_to_snowflake(df)
        except Exception as e:
            st.error(f"Error uploading Products: {e}")


# ------------------- SUPPLIER BY COUNTY -------------------
def render_supplier_county_section():
    st.subheader("Format Supplier by County")
    supplier_file = st.file_uploader("Upload Supplier by County Excel", type=["xlsx"], key="supplier_upload")

    if supplier_file:
        try:
            with st.spinner("Formatting Supplier by County..."):
                df = format_supplier_by_county(supplier_file)
            if df is not None:
                st.dataframe(df.head())
        except Exception as e:
            st.error(f"Error formatting Supplier by County: {e}")

    st.markdown("---")
    st.subheader("Upload Formatted Supplier by County to Snowflake")
    supplier_final = st.file_uploader("Re-upload formatted Supplier by County Excel", type=["xlsx"], key="supplier_final_upload")

    if supplier_final:
        try:
            df = format_supplier_by_county(supplier_final)
            st.dataframe(df.head())
            if st.button("Upload to Snowflake", key="upload_supplier_btn"):
                write_supplier_by_county_to_snowflake(df)
        except Exception as e:
            st.error(f"Error uploading Supplier by County: {e}")
