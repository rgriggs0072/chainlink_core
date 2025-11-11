# utils/logout_utils.py

import streamlit as st
import extra_streamlit_components as stx

def clear_auth_cookie(cookie_name="chainlink_token"):
    cookie_manager = stx.CookieManager()
    cookies = cookie_manager.get_all()
    if cookie_name in cookies:
        cookie_manager.delete(cookie_name)


def handle_logout(authenticator, cookie_name="chainlink_token"):
    st.session_state.pop("display_name", None)
    """Logs out the user, clears cookies and session state, and reruns the app."""
    if authenticator.logout("Logout", "sidebar", key="logout_key"):
        clear_auth_cookie(cookie_name)
        st.session_state.clear()
        st.rerun()

