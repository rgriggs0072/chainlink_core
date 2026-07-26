from PIL import Image
import os
import streamlit as st
import pandas as pd

from io import BytesIO
from utils.dashboard_data.home_dashboard import fetch_supplier_names
from utils.load_company_data_helpers import validate_store_numbers_for_chain

FALLBACK_LOGO_PATH = "images/Default_Logo/default_logo.png"

@st.cache_resource
def load_logo(full_path, max_width):
    try:
        img = Image.open(full_path)
        w, h = img.size
        aspect_ratio = h / w
        new_height = int(max_width * aspect_ratio)
        return img.resize((max_width, new_height))
    except Exception as e:
        if "logo_warned" not in st.session_state:
            print(f"[logo] Failed to load logo at {full_path}: {e}")
            st.session_state["logo_warned"] = True
        return None

def add_logo(logo_path, width=240):  # Width only — height is auto-computed
    if not logo_path or logo_path.strip() == "":
        logo_path = FALLBACK_LOGO_PATH

    if logo_path.startswith("./"):
        logo_path = logo_path[2:]

    full_path = os.path.join(os.getcwd(), logo_path)

    if "logo_printed" not in st.session_state:
        print(f"[logo] Trying to load logo at: {full_path}")
        st.session_state["logo_printed"] = True

    image = load_logo(full_path, width)
    if image is None and logo_path != FALLBACK_LOGO_PATH:
        fallback_full = os.path.join(os.getcwd(), FALLBACK_LOGO_PATH)
        image = load_logo(fallback_full, width)

    return image






def render_supplier_filter():
    conn = st.session_state.get("conn")
    if not conn:
        return

    try:
        supplier_options = fetch_supplier_names(conn)
        supplier_options.sort()
        supplier_options.insert(0, "All")

       # st.markdown("### 📦 Filter Suppliers")
        selected = st.multiselect(
            "Choose Suppliers",
            supplier_options[1:],  # Skip "All"
            default=st.session_state.get("selected_suppliers", supplier_options[1:3]),
            max_selections=5,
            key="supplier_selector"
        )
        st.session_state["selected_suppliers"] = selected
    except Exception as e:
        st.error("❌ Failed to load supplier options")
        st.exception(e)




def download_workbook(workbook, filename):
    stream = BytesIO()
    workbook.save(stream)
    stream.seek(0)
    st.download_button(
        label="Download formatted file",
        data=stream.read(),
        file_name=filename,
        mime='application/vnd.ms-excel'
    )


def apply_store_number_guardrail(
    df: pd.DataFrame,
    chain_name: str,
    tenant_id: str,
    conn,
) -> bool:
    """
    Runs validate_store_numbers_for_chain() and renders the result.

    Shared by Distro Grid and Reset Schedule upload flows.

    - Zero store numbers found: st.error() + st.stop().
    - Match rate below threshold: st.error() (with suggested chain, if
      any) + st.stop().
    - Match rate at/above threshold: subtle st.success() and returns True.

    st.stop() halts the entire script run, so callers can treat a True
    return as "safe to proceed" without needing to separately branch on
    a False return.
    """
    result = validate_store_numbers_for_chain(
        df=df,
        chain_name=chain_name,
        tenant_id=tenant_id,
        conn=conn,
    )

    chain_upper = chain_name.strip().upper()

    if result["total_count"] == 0:
        st.error("❌ No STORE_NUMBER values found in the uploaded file.")
        st.stop()

    if result["passed"]:
        st.success(
            f"✅ {result['matched_count']} of {result['total_count']} store numbers "
            f"verified for {chain_upper}."
        )
        return True

    match_pct = result["match_rate"] * 100
    message_lines = [
        "❌ Store number mismatch — upload stopped.",
        "",
        f"Only {result['matched_count']} of {result['total_count']} store numbers in your "
        f"file match {chain_upper} stores in the system ({match_pct:.0f}% match rate). "
        "The minimum required is 90%.",
        "",
    ]

    if result["suggested_chain"]:
        suggested_pct = result["suggested_match_rate"] * 100
        suggested_matched = round(result["suggested_match_rate"] * result["total_count"])
        message_lines.append(
            f"💡 Best match found: {result['suggested_chain']} "
            f"({suggested_matched} of {result['total_count']} store numbers match, "
            f"{suggested_pct:.0f}%). Did you mean to select {result['suggested_chain']} instead?"
        )
        message_lines.append("")
        message_lines.append(
            "Please select the correct chain and re-upload, or check that you have the right file."
        )
    else:
        message_lines.append(
            "No close match was found in any other chain. Please check that you have "
            f"uploaded the correct file for {chain_upper}."
        )

    st.error("\n".join(message_lines))
    st.stop()
    return False

