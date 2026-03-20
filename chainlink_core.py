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
- streamlit-authenticator 0.4.2: login() no longer returns a tuple;
  results are stored in st.session_state["name"], ["authentication_status"], ["username"].
- credentials are cached in session_state to prevent repeated Snowflake hits on every rerun.
"""

import streamlit as st
from PIL import Image
import streamlit_authenticator as stauth
import extra_streamlit_components as stx
from streamlit.components.v1 import html

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
from app_pages.predictive_purchases import render as predictive_purchases_page
from app_pages import driver_forecast

from nav.navigation_bar import (
    render_navigation,
    render_format_upload_submenu,
    render_reports_submenu,
    render_ai_forecasts_submenu,
    render_admin_submenu,
)
from nav.task_indicator import render_task_indicator, render_task_sidebar_card


def _safe_import(module_path: str):
    """Lazy-import a page module by dotted path."""
    import importlib
    return importlib.import_module(module_path)


# ---------------- Page Config ----------------
# IMPORTANT: set_page_config must be the FIRST Streamlit call.
# Do NOT put any st.markdown() or other st calls before main() runs.
st.set_page_config(
    page_title="Chainlink Analytics",
    page_icon="",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------- Session State Init ----------------
# Initialize early so guards work correctly on first load
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


# ---------------- Global Styles (injected once after auth check) ----------------
def _inject_global_styles():
    """
    Inject global CSS only once per session after page config.
    Calling this inside main() prevents style injection on login page rerenders.
    """
    st.markdown("""
    <style>
        .block-container {
            padding-top: 0rem;
            padding-bottom: 0rem;
            padding-left: 5rem;
            padding-right: 5rem;
        }
        h1 { font-size: 1.75rem !important; }
        #MainMenu, footer { visibility: hidden; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <style>
    :root {
        --primary-color: #6497D6;
        --secondary-color: #B3D7ED;
        --background-color: #F8F2EB;
    }
    h1, h2, h3 { color: var(--primary-color) !important; }
    div[data-testid="stDataFrameContainer"] table {
        border-radius: 8px;
        overflow: hidden;
    }
    .stDownloadButton button {
        background-color: var(--primary-color);
        color: white !important;
        border: none;
        border-radius: 6px;
        font-weight: 500;
    }
    .stDownloadButton button:hover { background-color: #4c7dc0 !important; }
    </style>
    """, unsafe_allow_html=True)


def hide_sidebar():
    st.markdown("""
    <style>
    section[data-testid="stSidebar"] { display: none !important; }
    div[data-testid="stToolbar"]      { display: none !important; }
    </style>
    """, unsafe_allow_html=True)


def clear_auth_cookie():
    """Remove auth cookie created by streamlit_authenticator."""
    cookie_manager = stx.CookieManager()
    cookie_manager.delete("chainlink_token")


# ---------------- Credentials cache ----------------
def _get_credentials() -> dict:
    """
    Fetch user credentials from Snowflake, cached in session_state.
    Prevents repeated DB hits on every Streamlit rerun during login.
    """
    if "cached_credentials" not in st.session_state:
        st.session_state["cached_credentials"] = fetch_user_credentials()
    return st.session_state["cached_credentials"]


# ---------------- Display Name Helpers ----------------
def _fetch_user_full_name_db(email: str, tenant_id: str) -> str:
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
    cache_key = "display_name"
    if st.session_state.get(cache_key):
        return st.session_state[cache_key]
    name = _fetch_user_full_name_db(email, tenant_id)
    st.session_state[cache_key] = name
    return name


# ---------------- Login Status Probe ----------------
def _probe_user_status(email: str) -> tuple[bool | None, bool | None, bool]:
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
            st.image(image, width=160)
        else:
            st.warning("Logo not available")

        st.success(f"Welcome, {display_name}!")
        handle_logout(authenticator)
        render_task_sidebar_card(
            conn=st.session_state.get("conn"),
            tenant_id=st.session_state.get("tenant_id"),
        )
        st.markdown("---")
        st.markdown(
            "<div style='font-size: 0.65rem; color: gray;'>© 2025 Chainlink Analytics LLC. All rights reserved.</div>",
            unsafe_allow_html=True,
        )


# ---------------- Admin Flag Helper ----------------
def _refresh_admin_flag():
    email  = st.session_state.get("user_email")
    tenant = st.session_state.get("tenant_id")
    st.session_state["is_admin"] = bool(
        email and tenant and is_admin_user(email, tenant)
    )


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
def main():
    """
    Auth → tenant context → nav → router.
    Admin-only: AI & Forecasts (with server-side guard).
    """
    # Inject global styles here (not at module level) to avoid
    # style flicker/jumping on the login page
    _inject_global_styles()

    # Use cached credentials — prevents Snowflake hit on every rerun
    credentials = _get_credentials()

    authenticator = stauth.Authenticate(
        credentials,
        "chainlink_token",
        COOKIE_KEY,
        cookie_expiry_days=0.014,
        auto_hash=False,
    )

    # 0.4.2: login() renders the form and stores results in session_state.
    # Skip rendering the login form entirely if already authenticated —
    # prevents the login form appearing alongside the dashboard after login.
    auth_status = st.session_state.get("authentication_status")
    username    = st.session_state.get("username")
    name        = st.session_state.get("name")

    if auth_status is not True:
        # Not yet authenticated — render login form and STOP.
        # The cookie manager (extra_streamlit_components) fires 1-2 reruns
        # on page load while checking for an existing session cookie.
        # We show a spinner during that check so the page doesn't appear
        # to jump while the cookie check resolves.

        # Check if this is a cookie-check rerun (no form interaction yet)
        # by looking for the stx cookie manager rerun marker
        is_cookie_checking = not st.session_state.get("_cookie_check_done")

        if is_cookie_checking:
            # Mark as done after first render so spinner only shows once
            st.session_state["_cookie_check_done"] = True
            with st.spinner("Loading Chainlink Analytics..."):
                # Small pause to let cookie check complete before rendering form
                import time
                time.sleep(0.4)
            st.rerun()

        hide_sidebar()
        authenticator.login(location="main")

        # Re-read after form render
        auth_status = st.session_state.get("authentication_status")
        username    = st.session_state.get("username")
        name        = st.session_state.get("name")

        if auth_status is True:
            # Login just succeeded — rerun for clean dashboard render
            st.rerun()
        elif auth_status is False:
            # Wrong credentials — handled in failure branch below
            pass
        else:
            # Waiting for input — stop here, render nothing else
            st.warning("Please enter your username and password")
            with st.expander("Forgot your password?"):
                if st.button("Reset Password Link"):
                    st.session_state["forgot_password_submitted"] = True
                    st.rerun()
            st.stop()

    # ── SUCCESSFUL LOGIN ──────────────────────────────────────────────────────
    if auth_status is True:
        username_lc = (username or "").strip().lower()
        user_entry  = credentials.get("usernames", {}).get(username_lc)
        if not user_entry or not user_entry.get("tenant_id"):
            st.error("Login error: user or tenant data missing")
            return

        if not is_user_active(username_lc, user_entry["tenant_id"]):
            st.error("Your account is disabled. Contact your administrator.")
            return
        if is_user_locked_out(username_lc):
            st.error("Your account is locked. Please contact your administrator.")
            return

        # Clear cookie check flag so spinner shows on next login
        st.session_state["_cookie_check_done"] = False

        # Set session context (only if not already set — prevents rerun cascade)
        if not st.session_state.get("authenticated"):
            st.session_state["authenticated"] = True
            st.session_state["user_email"]    = username_lc
            st.session_state["tenant_id"]     = user_entry["tenant_id"]

            tenant_config = load_tenant_config(user_entry["tenant_id"])
            if not isinstance(tenant_config, dict):
                st.error("Tenant configuration failed to load or is not a dict.")
                return

            st.session_state["tenant_config"] = tenant_config
            st.session_state["toml_info"]     = tenant_config

            required_keys = ["snowflake_user", "account", "private_key", "warehouse", "database", "schema"]
            missing = [k for k in required_keys if not tenant_config.get(k)]
            if missing:
                st.error(f"TOML configuration is incomplete. Missing: {', '.join(missing)}")
                return

            st.session_state["conn"] = connect_to_tenant_snowflake(tenant_config)
            reset_failed_attempts(username_lc)
            _refresh_admin_flag()

        # Read from session (whether just set or already cached)
        tenant_config = st.session_state.get("tenant_config", {})
        display_name  = _get_user_full_name_cached(
            st.session_state["user_email"],
            st.session_state["tenant_id"]
        ) or name or username_lc

        # Task indicator (above nav) — guarded inside the function
        render_task_indicator(
            conn=st.session_state["conn"],
            tenant_id=st.session_state["tenant_id"],
        )

        # Sidebar + top nav
        render_sidebar_header(display_name, tenant_config, authenticator)
        is_admin      = bool(st.session_state.get("is_admin"))
        selected_main = render_navigation(show_admin=is_admin, show_ai=is_admin)

        if not selected_main:
            st.error("Navigation menu failed to render or returned no selection.")
            return

        # ── Routing ──────────────────────────────────────────────────────────
        if selected_main == "Home":
            _safe_import("app_pages.home").render()
            return

        if selected_main == "Reports":
            report_page = render_reports_submenu()
            route = {
                "Gap Report":       "app_pages.gap_report",
                "Email Gap Report": "app_pages.email_gap_report",
                "Gap History":      "app_pages.gap_history",
                "Data Exports":     "app_pages.data_exports",
            }.get(report_page)
            if not route:
                st.warning("Invalid report selection.")
                return
            _safe_import(route).render()
            return

        if selected_main == "Format and Upload":
            selected_sub = render_format_upload_submenu()
            route = {
                "Load Company Data":            "app_pages.load_company_data",
                "Reset Schedule Processing":    "app_pages.reset_schedule",
                "Distribution Grid Processing": "app_pages.distro_grid",
            }.get(selected_sub)
            if not route:
                st.warning("Invalid format/upload selection.")
                return
            _safe_import(route).render()
            return

        if selected_main == "AI & Forecasts":
            if not is_admin:
                st.warning("You don't have access to AI & Forecasts.")
                st.rerun()
            selected_ai = render_ai_forecasts_submenu()
            ai_pages = {
                "Predictive Purchases":   "app_pages.predictive_purchases",
                "Predictive Truck Plan":  "app_pages.predictive_truck_plan",
                "AI-Narrative Report":    "app_pages.ai_narrative_report",
                "Placement Intelligence": "app_pages.ai_placement_intelligence",
            }
            module_path = ai_pages.get(selected_ai)
            if not module_path:
                st.error(f"Unknown selection from AI & Forecasts menu: {selected_ai!r}")
                st.info(f"Valid options: {', '.join(ai_pages.keys())}")
            else:
                _safe_import(module_path).render()
            st.stop()

        if selected_main == "Admin":
            if not is_admin:
                st.warning("You don't have access to Admin.")
                st.rerun()
            admin_page = render_admin_submenu()
            route = {
                "Admin Dashboard":      "app_pages.admin",
                "Sales Contacts Admin": "app_pages.sales_contacts_admin",
            }.get(admin_page)
            if not route:
                st.warning("Invalid admin selection.")
                return
            _safe_import(route).render()
            return

        st.warning("Unknown menu selection.")

    # ── FAILED LOGIN ──────────────────────────────────────────────────────────
    elif auth_status is False:
        email_lc = (username or "").strip().lower()
        if not email_lc:
            st.error("Username or password incorrect")
            return
        is_active, is_locked, exists = _probe_user_status(email_lc)
        if exists and is_active is False:
            st.error("Your account is disabled. Contact your administrator.")
            return
        if exists and is_locked:
            st.error("Your account is locked. Please contact your administrator.")
            return
        increment_failed_attempts(email_lc)
        if is_user_locked_out(email_lc):
            st.error("Account locked due to too many failed login attempts.")
        else:
            st.error("Username or password incorrect")

    # ── NOT YET LOGGED IN ─────────────────────────────────────────────────────
    # Handled above with st.stop() to prevent dashboard from rendering


if __name__ == "__main__":
    main()
