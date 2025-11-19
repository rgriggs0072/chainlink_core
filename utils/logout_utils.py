# utils/logout_utils.py
# -*- coding: utf-8 -*-
"""
Logout Utilities

Overview
--------
- Provides a safe, centralized logout flow for Chainlink Core.
- Wraps streamlit_authenticator.logout() to handle cases where the
  auth cookie (e.g., 'chainlink_token') is already missing/expired.
- Ensures Streamlit session_state is cleared on logout so the user
  is truly logged out even if the underlying cookie is gone.
"""

from __future__ import annotations

import streamlit as st

# Keys we want cleared when the user logs out.
AUTH_SESSION_KEYS = [
    "authentication_status",
    "name",
    "username",
    "tenant_id",
    "tenant_config",
    "conn",
    "tenant_db",
    "tenant_schema",
    "nav_selection",
    "display_name",
]


def _clear_auth_session() -> None:
    """Remove only authentication-related session keys."""
    for key in AUTH_SESSION_KEYS:
        st.session_state.pop(key, None)


def handle_logout(authenticator, cookie_name="chainlink_token") -> None:
    """
    Robust logout logic.

    Handles:
    - Normal logout
    - Cookie already expired → KeyError('chainlink_token')
    - Clears Streamlit session properly
    - Avoids app crash
    - Forces rerun back to login screen
    """
    if authenticator is None:
        return

    try:
        did_logout = authenticator.logout("Logout", "sidebar", key="logout_key")

        if did_logout:
            _clear_auth_session()
            st.rerun()

    except KeyError as e:
        # Most common failure: cookie already gone
        missing = str(e).strip("'\"")
        if missing == cookie_name:
            # Safe logout handling
            _clear_auth_session()
            st.warning("Your session had already expired. Logging you out.")
            st.rerun()
        else:
            st.error(f"Unexpected logout error (missing key: {missing}). Please refresh.")

    except Exception as e:
        st.error("An unexpected error occurred while logging out. Please refresh.")
        # Optional: show traceback only in dev
        if st.secrets.get("environment", "").lower() == "dev":
            st.exception(e)
