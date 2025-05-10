# chainlink_core.py

import streamlit as st
from PIL import Image
import streamlit_authenticator as stauth
import extra_streamlit_components as stx
from utils.logout_utils import handle_logout
from utils.ui_helpers import add_logo
from tenants.tenant_manager import load_tenant_config
from sf_connector.service_connector import connect_to_tenant_snowflake
from auth.login import fetch_user_credentials
from auth.reset_password import reset_password
from auth.forgot_password import forgot_password
from utils.auth_utils import is_user_locked_out, increment_failed_attempts, reset_failed_attempts

# 👇 NAV IMPORT
from nav.navigation_bar import render_navigation, render_format_upload_submenu, render_reports_submenu

# --- Page Config ---
st.set_page_config(
    page_title="Chainlink Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
    <style>
       section[data-testid="stSidebar"] {
            width: 250px !important;
            max-width: 250px !important;
            min-width: 250px !important;
        }

        .block-container {
            padding-top: 0rem;
            padding-bottom: 0rem;
            padding-left: 5rem;
            padding-right: 5rem;
        }
        h1 { font-size: 1.75rem !important; }
        header[data-testid="stHeader"] { visibility: hidden; }
        #MainMenu, footer {visibility: hidden;}
    </style>
""", unsafe_allow_html=True)

# --- Init Session State ---
for key in ["authenticated", "tenant_id", "user_email", "conn"]:
    if key not in st.session_state:
        st.session_state[key] = None

COOKIE_KEY = st.secrets["cookie_key"]["cookie_secret_key"]

# --- Token / Password Reset Handling ---
query_params = st.query_params
if query_params.get("token"):
    reset_password()
    st.stop()

if st.session_state.get("forgot_password_submitted"):
    forgot_password()
    st.stop()


def clear_auth_cookie():
    cookie_manager = stx.CookieManager()
    cookie_manager.delete("chainlink_token")  # 👈 this must match the cookie name in Authenticate()


# --- Sidebar Header + Logout Only ---
def render_sidebar_header(username, tenant_config, authenticator):
    with st.sidebar:
        logo_path = tenant_config.get("logo_path", "")
        image = add_logo(logo_path, width=160)
        if image:
            st.image(image, use_container_width=False)
        else:
            st.warning("⚠️ Logo not available")

        st.success(f"Welcome, {username}!")

        handle_logout(authenticator)  # 👈 Clean, centralized call

        st.markdown("---")
        st.markdown(
            "<div style='font-size: 0.65rem; color: gray;'>"
            "© 2025 Chainlink Analytics LLC. All rights reserved."
            "</div>",
            unsafe_allow_html=True
        )


# --- Main App Logic ---
def main():
    credentials = fetch_user_credentials()

    authenticator = stauth.Authenticate(
        credentials,
        "chainlink_token",
        COOKIE_KEY,
        cookie_expiry_days=0.014
    )

    name, auth_status, username = authenticator.login("Login", "main")

    if auth_status:
        username_lc = username.strip().lower()
        user_entry = credentials.get("usernames", {}).get(username_lc)

        if not user_entry or not user_entry.get("tenant_id"):
            st.error("Login error: user or tenant data missing")
            return

        if is_user_locked_out(username_lc):
            st.error("🚫 Your account is locked. Please contact your administrator.")
            return

        # ✅ Auth OK
        st.session_state["authenticated"] = True
        st.session_state["user_email"] = username_lc
        st.session_state["tenant_id"] = user_entry["tenant_id"]

        tenant_config = load_tenant_config(user_entry["tenant_id"])
        if not isinstance(tenant_config, dict):
            st.error("❌ Tenant configuration failed to load or is not a dict.")
            return

        required_keys = ["snowflake_user", "account", "private_key", "warehouse", "database", "schema"]
        missing_keys = [k for k in required_keys if k not in tenant_config or not tenant_config[k]]

        if missing_keys:
            st.error(f"❌ TOML configuration is incomplete. Missing: {', '.join(missing_keys)}")
            st.code({k: v for k, v in tenant_config.items() if "key" not in k.lower()}, language="json")
            return

        st.session_state["toml_info"] = tenant_config
        st.session_state["conn"] = connect_to_tenant_snowflake(tenant_config)
        reset_failed_attempts(username_lc)

       
        render_sidebar_header(username, tenant_config, authenticator)
        


        selected_main = render_navigation()
        if not selected_main:
            st.error("⚠️ Navigation menu failed to render or returned no selection.")
            return

        if selected_main == "Home":
            import app_pages.home as page
            page.render()

        elif selected_main == "Reports":
            report_page = render_reports_submenu()
            if report_page == "Gap Report":
                import app_pages.gap_report as page
                page.render()
            elif report_page == "Data Exports":
                import app_pages.data_exports as page
                page.render()
            elif report_page == "AI-Narrative Report":
                import app_pages.ai_narrative_report as page
                page.render()
            else:
                st.warning("Invalid report selection.")


        elif selected_main == "Format and Upload":
            selected_sub = render_format_upload_submenu()
            if selected_sub == "Load Company Data":
                import app_pages.load_company_data as page
            elif selected_sub == "Reset Schedule Processing":
                import app_pages.reset_schedule as page
            elif selected_sub == "Distribution Grid Processing":
                import app_pages.distro_grid as page
            page.render()

        elif selected_main == "Admin":
            import app_pages.admin as page
            page.render()

    elif auth_status is False:
        email_lc = (username or "").strip().lower()
        if email_lc:
            increment_failed_attempts(email_lc)
            if is_user_locked_out(email_lc):
                st.error("🚫 Account locked due to too many failed login attempts.")
            else:
                st.error("Username or password incorrect")
        else:
            st.error("Username or password incorrect")

    elif auth_status is None:
        st.warning("Please enter your username and password")
        with st.expander("Forgot your password?"):
            if st.button("Reset Password Link"):
                st.session_state["forgot_password_submitted"] = True
                st.rerun()

if __name__ == "__main__":
    main()
