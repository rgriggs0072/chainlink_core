import streamlit as st

def render():
    st.title("📦 Upload Product Catalog")
    st.write("Use this page to upload product catalogs and map fields to system format.")

    uploaded_file = st.file_uploader("Upload product catalog (.xlsx, .csv)", type=["xlsx", "csv"])
    if uploaded_file:
        st.success(f"Catalog file '{uploaded_file.name}' uploaded successfully (placeholder).")

