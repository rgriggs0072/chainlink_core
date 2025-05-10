import streamlit as st

def render():
    st.title("🧾 Import Distributor Report")
    st.write("This is the interface to upload and validate distributor report files.")

    uploaded_file = st.file_uploader("Upload distributor report (.xlsx, .csv)", type=["xlsx", "csv"])
    if uploaded_file:
        st.success(f"File '{uploaded_file.name}' uploaded successfully (placeholder).")

