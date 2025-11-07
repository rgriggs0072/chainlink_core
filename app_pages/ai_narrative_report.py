# app_pages/ai_narrative_report.py

import streamlit as st
import pandas as pd
from io import BytesIO
from utils.pdf_utils import generate_ai_report_pdf
from sf_connector.service_connector import connect_to_tenant_snowflake
import openai

# Load OpenAI key
OPENAI_API_KEY = st.secrets["openai"]["api_key"]

def get_summary_data(conn, store_name):
    sales_query = f"""
        SELECT PRODUCT_NAME, COUNT(*) AS TOTAL_ATTEMPTS,
        SUM(PURCHASED_YES_NO) AS PURCHASED,
        ROUND(SUM(PURCHASED_YES_NO) / COUNT(*), 2) AS PURCHASE_RATE
        FROM SALES_REPORT
        WHERE STORE_NAME = '{store_name}'
        GROUP BY PRODUCT_NAME
        ORDER BY PURCHASED DESC
        LIMIT 5
    """
    gaps_query = f"""
        SELECT COUNTY, COUNT(*) AS TOTAL_GAPS
        FROM GAP_REPORT
        WHERE STORE_NAME = '{store_name}'
        GROUP BY COUNTY
        ORDER BY TOTAL_GAPS DESC
        LIMIT 5
    """
    sales_df = pd.read_sql(sales_query, conn)
    gaps_df = pd.read_sql(gaps_query, conn)
    return sales_df, gaps_df

def generate_narrative(sales_df, gaps_df):
    client = openai.OpenAI(api_key=OPENAI_API_KEY)

    sales_summary = sales_df.to_string(index=False)
    gaps_summary = gaps_df.to_string(index=False)

    prompt = f"""
    You are a retail data analyst. Analyze the sales and gap data below and provide a narrative summary highlighting:

    - Top purchased products
    - Purchase conversion rates
    - Counties with the most gaps
    - Suggested actions to reduce gaps or improve execution

    Sales Summary:
    {sales_summary}

    Gap Summary:
    {gaps_summary}
    """

    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are a retail data analyst."},
                {"role": "user", "content": prompt}
            ]
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"\u274c AI Generation Failed: {e}"

def render():
    st.title("\U0001F8BE AI Narrative Report")
    st.markdown("Generate a narrative summary of key sales and gap trends using AI.")

    toml_info = st.session_state.get("toml_info")
    if not toml_info:
        st.error("Missing tenant configuration. Please log in again.")
        return

    try:
        conn = connect_to_tenant_snowflake(toml_info)
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT STORE_NAME FROM CUSTOMERS ORDER BY STORE_NAME")
            store_options = [row[0] for row in cur.fetchall()]
    except Exception as e:
        st.error(f"Error loading store names: {e}")
        return

    with st.form("ai_narrative_form"):
        store_name = st.selectbox("Select Store Name", ["-- Select Store --"] + store_options)
        submitted = st.form_submit_button("Generate AI-Narrative Report")

    if submitted and store_name != "-- Select Store --":
        with st.spinner("Analyzing data and generating AI report..."):
            conn = connect_to_tenant_snowflake(toml_info)
            if not conn:
                st.error("Snowflake connection failed.")
                return

            sales_df, gaps_df = get_summary_data(conn, store_name)
            if sales_df.empty or gaps_df.empty:
                st.warning("No data found for the selected store.")
                return

            report_text = generate_narrative(sales_df, gaps_df)
            client_name = toml_info.get("client_name", "Chainlink Client")
            pdf_buffer = generate_ai_report_pdf(client_name, store_name, report_text)

            st.session_state["report_text"] = report_text
            st.session_state["pdf_buffer"] = pdf_buffer
            st.session_state["store_name"] = store_name

    if "report_text" in st.session_state:
        st.subheader("\U0001F4E0 AI-Generated Narrative")
        st.write(st.session_state["report_text"])

        st.download_button(
            label="\U0001F4C4 Download Narrative Report (PDF)",
            data=st.session_state["pdf_buffer"],
            file_name=f"{st.session_state['store_name']}_narrative_report.pdf",
            mime="application/pdf"
        )
