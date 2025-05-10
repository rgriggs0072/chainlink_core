# utils/logout_utils.py

import streamlit as st
import extra_streamlit_components as stx

def clear_auth_cookie(cookie_name="chainlink_token"):
    cookie_manager = stx.CookieManager()
    cookie_manager.delete(cookie_name)

def handle_logout(authenticator, cookie_name="chainlink_token"):
    """Logs out the user, clears cookies and session state, and reruns the app."""
    if authenticator.logout("Logout", "sidebar", key="logout_key"):
        clear_auth_cookie(cookie_name)
        st.session_state.clear()
        st.rerun()

