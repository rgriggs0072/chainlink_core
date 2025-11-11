# ------------------ chainlink_core.py -------------------
"""
Chainlink Core - Main App Entrypoint

Overview for devs:
- Handles auth (streamlit_authenticator), tenant config load, and tenant Snowflake connection.
- Renders a top navigation bar with optional Admin tab based on USERDATA.ROLE.
- Uses form-driven pages elsewhere to minimize reruns; keeps hard server-side guards on admin routes.

Key notes:
- Admin visibility: computed every run via is_admin_user(email, tenant_id); cached in st.session_state["is_admin"].
- Navigation: pass show_admin=st.session_state["is_admin"] to render_navigation().
- Deep-link protection: verify admin before rendering Admin page.
"""

import streamlit as st
from PIL import Image
import streamlit_authenticator as stauth
import extra_streamlit_components as stx

from utils.logout_utils import handle_logout
from utils.ui_helpers import add_logo
from tenants.tenant_manager import load_tenant_config
from sf_connector.service_connector import connect_to_tenant_snowflake, get_service_account_connection

from auth.login import fetch_user_credentials
from auth.reset_password import reset_password
from auth.forgot_password import forgot_password
from utils.auth_utils import (
    is_user_active,
    is_user_locked_out,
    increment_failed_attempts,
    reset_failed_attempts,
    is_admin_user,
)

# Nav imports (render_navigation takes show_admin: bool)
from nav.navigation_bar import (
    render_navigation,
    render_format_upload_submenu,
    render_reports_submenu,
)

# ---------------- Page Config & Global Styles ----------------
st.set_page_config(
    page_title="Chainlink Analytics",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
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
    """,
    unsafe_allow_html=True,
)

# ---------------- Session State Init ----------------
for key in ["authenticated", "tenant_id", "user_email", "conn", "is_admin"]:
    if key not in st.session_state:
        st.session_state[key] = None

COOKIE_KEY = st.secrets["cookie_key"]["cookie_secret_key"]

# ---------------- Token / Password Reset Handling ----------------
query_params = st.query_params
if query_params.get("token"):
    reset_password()
    st.stop()

if st.session_state.get("forgot_password_submitted"):
    forgot_password()
    st.stop()

def clear_auth_cookie():
    """Remove auth cookie created by streamlit_authenticator."""
    cookie_manager = stx.CookieManager()
    cookie_manager.delete("chainlink_token")  # must match Authenticate() cookie name

# ---------------- Display Name Helpers ----------------
def _fetch_user_full_name_db(email: str, tenant_id: str) -> str:
    """
    One-shot DB fetch for FIRST_NAME + LAST_NAME via service account.
    """
    try:
        email_lc = (email or "").strip().lower()
        if not email_lc or not tenant_id:
            return email

        conn = get_service_account_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(FIRST_NAME, ''), COALESCE(LAST_NAME, '')
                    FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
                    WHERE LOWER(EMAIL) = %s AND TENANT_ID = %s
                    LIMIT 1
                """, (email_lc, tenant_id))
                row = cur.fetchone()
        finally:
            conn.close()

        if not row:
            return email
        first, last = (row[0] or "").strip(), (row[1] or "").strip()
        full = " ".join(x for x in (first, last) if x)
        return full or email
    except Exception:
        return email

def _get_user_full_name_cached(email: str, tenant_id: str) -> str:
    """
    Session-level cache for the display name.
    """
    cache_key = "display_name"
    if st.session_state.get(cache_key):
        return st.session_state[cache_key]
    name = _fetch_user_full_name_db(email, tenant_id)
    st.session_state[cache_key] = name
    return name

# ---------------- Login Status Probe (for failed logins) ----------------
def _probe_user_status(email: str) -> tuple[bool | None, bool | None, bool]:
    """
    Returns (is_active, is_locked, exists) by EMAIL across any tenant row.
    Used only for messaging in the failed-login branch to avoid incrementing attempts
    for disabled/locked accounts.
    """
    try:
        email_lc = (email or "").strip().lower()
        if not email_lc:
            return (None, None, False)
        conn = get_service_account_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COALESCE(IS_ACTIVE, TRUE), COALESCE(IS_LOCKED, FALSE)
                    FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
                    WHERE LOWER(EMAIL) = %s
                    LIMIT 1
                """, (email_lc,))
                row = cur.fetchone()
        finally:
            conn.close()
        if not row:
            return (None, None, False)
        return (bool(row[0]), bool(row[1]), True)
    except Exception:
        return (None, None, False)

# ---------------- Sidebar Header ----------------
def render_sidebar_header(display_name, tenant_config, authenticator):
    with st.sidebar:
        logo_path = tenant_config.get("logo_path", "")
        image = add_logo(logo_path, width=160)
        if image:
            st.image(image, width="content")
        else:
            st.warning("Logo not available")

        st.success(f"Welcome, {display_name}!")
        handle_logout(authenticator)

        st.markdown("---")
        st.markdown(
            "<div style='font-size: 0.65rem; color: gray;'>© 2025 Chainlink Analytics LLC. All rights reserved.</div>",
            unsafe_allow_html=True,
        )

# ---------------- Admin Flag Helper ----------------
def _refresh_admin_flag():
    """
    Compute and cache whether the current user is an admin for their tenant.
    Called after successful auth and tenant_id/user_email set.
    """
    email = st.session_state.get("user_email")
    tenant = st.session_state.get("tenant_id")
    st.session_state["is_admin"] = bool(email and tenant and is_admin_user(email, tenant))

# ---------------- Main ----------------
def main():
    credentials = fetch_user_credentials()

    authenticator = stauth.Authenticate(
        credentials,
        "chainlink_token",
        COOKIE_KEY,
        cookie_expiry_days=0.014,
    )

    name, auth_status, username = authenticator.login("Login", "main")

    # ---------- SUCCESSFUL LOGIN ----------
    if auth_status:
        username_lc = username.strip().lower()
        user_entry = credentials.get("usernames", {}).get(username_lc)

        if not user_entry or not user_entry.get("tenant_id"):
            st.error("Login error: user or tenant data missing")
            return

        # Belt & suspenders: ensure the account is still active & not locked
        if not is_user_active(username_lc, user_entry["tenant_id"]):
            st.error("Your account is disabled. Contact your administrator.")
            return
        if is_user_locked_out(username_lc):
            st.error("Your account is locked. Please contact your administrator.")
            return

        # Auth OK — establish session context
        st.session_state["authenticated"] = True
        st.session_state["user_email"] = username_lc
        st.session_state["tenant_id"] = user_entry["tenant_id"]

        # Load tenant configuration (TOML storage in Snowflake)
        tenant_config = load_tenant_config(user_entry["tenant_id"])
        if not isinstance(tenant_config, dict):
            st.error("Tenant configuration failed to load or is not a dict.")
            return

        st.session_state["tenant_config"] = tenant_config
        st.session_state["toml_info"] = tenant_config  # legacy compatibility

        # Validate TOML required fields
        required_keys = ["snowflake_user", "account", "private_key", "warehouse", "database", "schema"]
        missing_keys = [k for k in required_keys if k not in tenant_config or not tenant_config[k]]
        if missing_keys:
            st.error(f"TOML configuration is incomplete. Missing: {', '.join(missing_keys)}")
            st.code({k: v for k, v in tenant_config.items() if "key" not in k.lower()}, language="json")
            return

        # Connect to tenant Snowflake + reset failed attempts
        st.session_state["conn"] = connect_to_tenant_snowflake(tenant_config)
        reset_failed_attempts(username_lc)

        # Compute admin flag and resolve display name
        _refresh_admin_flag()
        display_name = _get_user_full_name_cached(username_lc, st.session_state["tenant_id"])
        if not display_name or display_name.lower() == username_lc.lower():
            display_name = name or username_lc  # fallback

        # Sidebar + nav
        render_sidebar_header(display_name, tenant_config, authenticator)

        selected_main = render_navigation(show_admin=bool(st.session_state.get("is_admin")))
        if not selected_main:
            st.error("Navigation menu failed to render or returned no selection.")
            return

        # ---------- Routing ----------
        if selected_main == "Home":
            import app_pages.home as page
            page.render()

        elif selected_main == "Reports":
            report_page = render_reports_submenu()
            if report_page == "Gap Report":
                import app_pages.gap_report as page
            elif report_page == "Data Exports":
                import app_pages.data_exports as page
            elif report_page == "AI-Narrative Report":
                import app_pages.ai_narrative_report as page
            elif report_page == "Placement Intelligence":
                import app_pages.ai_placement_intelligence as page
            elif report_page == "Email Gap Report":
                import app_pages.email_gap_report as page
            else:
                st.warning("Invalid report selection.")
                return
            page.render()

        elif selected_main == "Format and Upload":
            selected_sub = render_format_upload_submenu()
            if selected_sub == "Load Company Data":
                import app_pages.load_company_data as page
            elif selected_sub == "Reset Schedule Processing":
                import app_pages.reset_schedule as page
            elif selected_sub == "Distribution Grid Processing":
                import app_pages.distro_grid as page
            else:
                st.warning("Invalid format/upload selection.")
                return
            page.render()

        elif selected_main == "Admin":
            # Server-side guard: if somehow selected without rights, bounce
            if not st.session_state.get("is_admin", False):
                st.warning("You don’t have access to Admin.")
                st.rerun()
            import app_pages.admin as page
            page.render()

    # ---------- FAILED LOGIN ----------
    elif auth_status is False:
        email_lc = (username or "").strip().lower()
        if not email_lc:
            st.error("Username or password incorrect")
        else:
            # Probe status for clearer UX & to avoid incrementing on disabled/locked
            is_active, is_locked, exists = _probe_user_status(email_lc)

            if exists and is_active is False:
                st.error("Your account is disabled. Contact your administrator.")
            elif exists and is_locked:
                st.error("Your account is locked. Please contact your administrator.")
            else:
                # Only increment for genuine bad attempts on non-disabled/non-locked (or unknown) accounts
                increment_failed_attempts(email_lc)
                if is_user_locked_out(email_lc):
                    st.error("Account locked due to too many failed login attempts.")
                else:
                    st.error("Username or password incorrect")

    # ---------- NOT YET LOGGED IN ----------
    elif auth_status is None:
        st.warning("Please enter your username and password")
        with st.expander("Forgot your password?"):
            if st.button("Reset Password Link"):
                st.session_state["forgot_password_submitted"] = True
                st.rerun()

if __name__ == "__main__":
    main()
