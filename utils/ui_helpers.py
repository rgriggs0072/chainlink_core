from PIL import Image
import os
import streamlit as st

from io import BytesIO
from utils.dashboard_data.home_dashboard import fetch_supplier_names

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
            print(f"⚠️ Failed to load logo at {full_path}: {e}")
            st.session_state["logo_warned"] = True
        return None

def add_logo(logo_path, width=240):  # Width only — height is auto-computed
    if not logo_path or logo_path.strip() == "":
        logo_path = FALLBACK_LOGO_PATH

    if logo_path.startswith("./"):
        logo_path = logo_path[2:]

    full_path = os.path.join(os.getcwd(), logo_path)

    if "logo_printed" not in st.session_state:
        print(f"🔍 Trying to load logo at: {full_path}")
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

