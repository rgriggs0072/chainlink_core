# app_pages/admin.py
"""
Admin Page (Chainlink Core) — Form-Driven
-----------------------------------------
Overview for devs:
- Uses st.form per panel to prevent page-wide reruns on every keystroke.
- Matches current schema: USERDATA.ROLE, USERDATA.IS_LOCKED, USERDATA.IS_ACTIVE, FAILED_LOGINS.EMAIL.
- Uses utils.auth_utils signatures:
    is_admin_user(user_email, tenant_id)
    unlock_user_account(email, unlocked_by=None, tenant_id=None, reason="Manual unlock")
    create_user_account(conn, email, first_name, last_name, role_name, tenant_id)

New in this version:
- Manage User Status (Disable / Enable users) with guardrails.
- Delete Users with "type DELETE" confirmation and safety checks:
    * cannot act on self
    * cannot remove the last ADMIN in the tenant
"""

from __future__ import annotations
from datetime import datetime, timedelta
from typing import Iterable, List, Tuple
import streamlit as st
import pandas as pd

from sf_connector.service_connector import get_service_account_connection
from utils.auth_utils import (
    is_admin_user,
    unlock_user_account,
    create_user_account,
)

# ---------- lightweight data helpers (DB reads only) ----------

def _rows_to_df(rows, cols):
    return pd.DataFrame.from_records(list(rows), columns=cols)

def fetch_admin_metrics(con, tenant_id: str) -> dict:
    out = {"total_users": 0, "locked_accounts": 0, "resets_7d": 0, "failed_24h": 0}
    with con.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
            WHERE TENANT_ID = %s
        """, (tenant_id,))
        out["total_users"] = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
            WHERE TENANT_ID = %s AND COALESCE(IS_LOCKED, FALSE) = TRUE
        """, (tenant_id,))
        out["locked_accounts"] = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM TENANTUSERDB.CHAINLINK_SCH.RESET_LOGS
            WHERE TENANT_ID = %s AND TIMESTAMP >= DATEADD(day, -7, CURRENT_TIMESTAMP())
        """, (tenant_id,))
        out["resets_7d"] = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM TENANTUSERDB.CHAINLINK_SCH.FAILED_LOGINS
            WHERE TENANT_ID = %s AND TIMESTAMP >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
        """, (tenant_id,))
        out["failed_24h"] = cur.fetchone()[0]
    return out

def fetch_reset_logs(con, tenant_id: str, *, email=None, success=None,
                     dt_from=None, dt_to=None, limit=500) -> pd.DataFrame:
    sql = [
        "SELECT EMAIL, RESET_TOKEN, SUCCESS, TIMESTAMP, IP_ADDRESS, REASON",
        "FROM TENANTUSERDB.CHAINLINK_SCH.RESET_LOGS",
        "WHERE TENANT_ID = %s"
    ]
    params = [tenant_id]
    if email:
        sql.append("AND UPPER(EMAIL) = UPPER(%s)")
        params.append(email)
    if success in ("True", "False"):
        sql.append("AND SUCCESS = %s")
        params.append(success == "True")
    if dt_from:
        sql.append("AND TIMESTAMP >= %s")
        params.append(dt_from)
    if dt_to:
        sql.append("AND TIMESTAMP <= %s")
        params.append(dt_to)
    sql.append("ORDER BY TIMESTAMP DESC LIMIT %s")
    params.append(int(limit))
    with con.cursor() as cur:
        cur.execute(" ".join(sql), tuple(params))
        return _rows_to_df(cur.fetchall(), ["EMAIL","RESET_TOKEN","SUCCESS","TIMESTAMP","IP_ADDRESS","REASON"])

def fetch_failed_logins(con, tenant_id: str, *, email=None, dt_from=None, dt_to=None, limit=500) -> pd.DataFrame:
    sql = [
        "SELECT EMAIL, TIMESTAMP, IP_ADDRESS",
        "FROM TENANTUSERDB.CHAINLINK_SCH.FAILED_LOGINS",
        "WHERE TENANT_ID = %s"
    ]
    params = [tenant_id]
    if email:
        sql.append("AND UPPER(EMAIL) = UPPER(%s)")
        params.append(email)
    if dt_from:
        sql.append("AND TIMESTAMP >= %s")
        params.append(dt_from)
    if dt_to:
        sql.append("AND TIMESTAMP <= %s")
        params.append(dt_to)
    sql.append("ORDER BY TIMESTAMP DESC LIMIT %s")
    params.append(int(limit))
    with con.cursor() as cur:
        cur.execute(" ".join(sql), tuple(params))
        return _rows_to_df(cur.fetchall(), ["EMAIL","TIMESTAMP","IP_ADDRESS"])

def fetch_locked_users(con, tenant_id: str) -> pd.DataFrame:
    with con.cursor() as cur:
        cur.execute("""
            SELECT EMAIL
            FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
            WHERE TENANT_ID = %s AND COALESCE(IS_LOCKED, FALSE) = TRUE
            ORDER BY UPPER(EMAIL)
        """, (tenant_id,))
        return _rows_to_df(cur.fetchall(), ["EMAIL"])

def fetch_all_users(con, tenant_id: str) -> pd.DataFrame:
    """
    Returns Email, Role, IsActive, IsLocked for the tenant.
    """
    with con.cursor() as cur:
        cur.execute("""
            SELECT EMAIL, ROLE, COALESCE(IS_ACTIVE, TRUE) AS IS_ACTIVE, COALESCE(IS_LOCKED, FALSE) AS IS_LOCKED
            FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
            WHERE TENANT_ID = %s
            ORDER BY UPPER(EMAIL)
        """, (tenant_id,))
        return _rows_to_df(cur.fetchall(), ["EMAIL","ROLE","IS_ACTIVE","IS_LOCKED"])

def count_admins(con, tenant_id: str) -> int:
    with con.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
            WHERE TENANT_ID = %s AND UPPER(ROLE) = 'ADMIN' AND COALESCE(IS_ACTIVE, TRUE) = TRUE
        """, (tenant_id,))
        return int(cur.fetchone()[0])

# ---------- mutations (status / delete) ----------

def set_users_active(con, tenant_id: str, emails: Iterable[str], active: bool, actor_email: str | None = None, reason: str | None = None) -> int:
    """
    Enable/disable users by email in this tenant.
    - Disabling also clears if locked and resets attempts to avoid confusion later.
    - Logs each change in USER_STATUS_LOGS.
    Returns number of rows updated.
    """
    emails = [e.strip().lower() for e in emails if e]
    if not emails:
        return 0
    q_marks = ", ".join(["%s"] * len(emails))
    params = [tenant_id] + emails
    action = "ENABLE" if active else "DISABLE"

    with con.cursor() as cur:
        # --- Main update ---
        if active:
            cur.execute(f"""
                UPDATE TENANTUSERDB.CHAINLINK_SCH.USERDATA
                SET IS_ACTIVE = TRUE
                WHERE TENANT_ID = %s AND LOWER(EMAIL) IN ({q_marks})
            """, tuple(params))
        else:
            cur.execute(f"""
                UPDATE TENANTUSERDB.CHAINLINK_SCH.USERDATA
                SET IS_ACTIVE = FALSE, FAILED_ATTEMPTS = 0, IS_LOCKED = FALSE
                WHERE TENANT_ID = %s AND LOWER(EMAIL) IN ({q_marks})
            """, tuple(params))
        affected = cur.rowcount

        # --- Audit log insert (one per email) ---
        if actor_email and affected:
            for email in emails:
                cur.execute("""
                    INSERT INTO TENANTUSERDB.CHAINLINK_SCH.USER_STATUS_LOGS
                    (TENANT_ID, ACTOR_EMAIL, TARGET_EMAIL, ACTION, REASON)
                    VALUES (%s, %s, %s, %s, %s)
                """, (tenant_id, actor_email, email, action, reason or ""))

    con.commit()
    return affected


def delete_users(con, tenant_id: str, emails: Iterable[str], actor_email: str | None = None) -> int:
    """
    Hard delete user rows for this tenant.
    CALLERS MUST:
    - prevent self-deletion
    - ensure not deleting last ADMIN
    - Logs each deletion in USER_DELETE_LOGS.
    """
    emails = [e.strip().lower() for e in emails if e]
    if not emails:
        return 0
    q_marks = ", ".join(["%s"] * len(emails))
    params = [tenant_id] + emails

    with con.cursor() as cur:
        # --- Audit first (preserve who was deleted) ---
        if actor_email:
            for email in emails:
                cur.execute("""
                    INSERT INTO TENANTUSERDB.CHAINLINK_SCH.USER_DELETE_LOGS
                    (TENANT_ID, ACTOR_EMAIL, TARGET_EMAIL)
                    VALUES (%s, %s, %s)
                """, (tenant_id, actor_email, email))

        # --- Actual deletion ---
        cur.execute(f"""
            DELETE FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
            WHERE TENANT_ID = %s AND LOWER(EMAIL) IN ({q_marks})
        """, tuple(params))
        affected = cur.rowcount

    con.commit()
    return affected

# ---------- caches ----------
@st.cache_data(ttl=30, show_spinner=False)
def _get_metrics(tenant_id: str) -> dict:
    with get_service_account_connection() as con:
        return fetch_admin_metrics(con, tenant_id)

@st.cache_data(ttl=30, show_spinner=False)
def _get_reset_logs(tenant_id: str, email, success, dt_from, dt_to, limit) -> pd.DataFrame:
    with get_service_account_connection() as con:
        return fetch_reset_logs(con, tenant_id, email=email, success=success,
                                dt_from=dt_from, dt_to=dt_to, limit=limit)

@st.cache_data(ttl=30, show_spinner=False)
def _get_failed(tenant_id: str, email, dt_from, dt_to, limit) -> pd.DataFrame:
    with get_service_account_connection() as con:
        return fetch_failed_logins(con, tenant_id, email=email, dt_from=dt_from, dt_to=dt_to, limit=limit)

@st.cache_data(ttl=30, show_spinner=False)
def _get_locked(tenant_id: str) -> pd.DataFrame:
    with get_service_account_connection() as con:
        return fetch_locked_users(con, tenant_id)

@st.cache_data(ttl=30, show_spinner=False)
def _get_users(tenant_id: str) -> pd.DataFrame:
    with get_service_account_connection() as con:
        return fetch_all_users(con, tenant_id)

def _invalidate_caches():
    _get_metrics.clear()
    _get_reset_logs.clear()
    _get_failed.clear()
    _get_locked.clear()
    _get_users.clear()

def _metric_card(label: str, value):
    st.markdown("""
    <style>
    .metric-card { background:#f7f7f9;padding:16px;border-radius:14px;box-shadow:0 2px 8px rgba(0,0,0,0.06); }
    .metric-label { font-size:0.85rem;color:#555; }
    .metric-value { font-size:1.8rem;font-weight:700;margin-top:6px; }
    </style>
    """, unsafe_allow_html=True)
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">{label}</div>
            <div class="metric-value">{value}</div>
        </div>
    """, unsafe_allow_html=True)

def _spacer(px=10):
    st.markdown(f"<div style='height:{px}px'></div>", unsafe_allow_html=True)

# ---------- REQUIRED EXPORT ----------
def render():
    """Entry point used by app_pages.__init__ to show the Admin page."""
    

    # session + role guard
    if "tenant_id" not in st.session_state or "user_email" not in st.session_state:
        st.error("No active session. Please log in.")
        st.stop()

    tenant_id = st.session_state["tenant_id"]
    user_email = st.session_state["user_email"]

    if not is_admin_user(user_email, tenant_id):
        st.warning("You don’t have admin permissions for this tenant.")
        st.stop()

    # --- title + metrics (no form) ---
    st.title("Admin")

    st.subheader("Metrics")
    m = _get_metrics(tenant_id) or {}
    c1, c2, c3, c4 = st.columns(4)
    with c1: _metric_card("Total Users", m.get("total_users", 0))
    with c2: _metric_card("Locked Accounts", m.get("locked_accounts", 0))
    with c3: _metric_card("Password Resets (7d)", m.get("resets_7d", 0))
    with c4: _metric_card("Failed Logins (24h)", m.get("failed_24h", 0))
    _spacer(8)


     # --- Create User (form) ---
    with st.expander("👤 Create User", expanded=False):
        with st.form("create_user_form", clear_on_submit=True):
            c1, c2 = st.columns(2)
            with c1:
                first_name = st.text_input("First Name")
                role_name  = st.selectbox("Role", ["USER", "ADMIN"], index=0)
            with c2:
                last_name  = st.text_input("Last Name")
                email_new  = st.text_input("Email")
            submit_create = st.form_submit_button("Create", type="primary")

        if submit_create:
            if not email_new or not first_name or not last_name:
                st.error("Email, First Name, and Last Name are required.")
            else:
                try:
                    with get_service_account_connection() as con:
                        ok, msg = create_user_account(
                            con,
                            email=email_new,
                            first_name=first_name,
                            last_name=last_name,
                            role_name=role_name,
                            tenant_id=tenant_id,
                        )
                    if ok:
                        st.success(msg)
                        _invalidate_caches()
                    else:
                        st.warning(msg)
                except Exception as e:
                    st.error(f"Create failed: {e}")


 # --- Manage User Status (Disable / Enable) ---
    with st.expander("⏯️ Disable / Enable Users", expanded=False):
        with st.form("status_users_form", clear_on_submit=True):
            users_df = _get_users(tenant_id)
            active_users = users_df[users_df["IS_ACTIVE"] == True]["EMAIL"].tolist()
            inactive_users = users_df[users_df["IS_ACTIVE"] == False]["EMAIL"].tolist()

            c1, c2 = st.columns(2)
            with c1:
                to_disable = st.multiselect("Active users to disable", active_users)
            with c2:
                to_enable = st.multiselect("Inactive users to enable", inactive_users)

            enforce_admin_guard = st.checkbox("Prevent disabling the last ADMIN", value=True)
            submit_status = st.form_submit_button("Apply Changes", type="primary")

        if submit_status:
            if not to_disable and not to_enable:
                st.info("No changes selected.")
            else:
                with get_service_account_connection() as con:
                    # Self-protection: never allow acting on yourself
                    me = user_email.strip().lower()
                    if me in [e.strip().lower() for e in to_disable]:
                        st.error("You cannot disable your own account.")
                        st.stop()

                    # Last-admin guard if requested
                    if enforce_admin_guard and to_disable:
                        # count remaining admins after disabling any admins in selection
                        # Map selected emails → roles
                        users_df = fetch_all_users(con, tenant_id)
                        by_email = {r["EMAIL"].strip().lower(): r["ROLE"].strip().upper() for _, r in users_df.iterrows()}
                        admins_selected = [e for e in to_disable if by_email.get(e.strip().lower()) == "ADMIN"]
                        if admins_selected:
                            current_admins = count_admins(con, tenant_id)
                            remaining = current_admins - len(admins_selected)
                            if remaining <= 0:
                                st.error("Blocked: This action would disable the last ADMIN in the tenant.")
                                st.stop()

                    rows_up = 0
                    rows_up += set_users_active(con, tenant_id, to_disable, active=False) if to_disable else 0
                    rows_up += set_users_active(con, tenant_id, to_enable, active=True) if to_enable else 0
                    con.commit()

                st.success(f"Status updated for {rows_up} user(s).")
                _invalidate_caches()
                st.rerun()



    # --- Delete Users (Hard delete with confirmation) ---
    with st.expander("🗑️ Delete Users", expanded=False):
        with st.form("delete_users_form", clear_on_submit=True):
            users_df = _get_users(tenant_id)
            all_emails = users_df["EMAIL"].tolist()
            victims = st.multiselect("Users to delete (cannot be undone)", all_emails)

            c1, c2 = st.columns(2)
            with c1:
                confirm_text = st.text_input('Type "DELETE" to confirm')
            with c2:
                enforce_admin_guard_del = st.checkbox("Prevent deleting the last ADMIN", value=True)

            submit_delete = st.form_submit_button("Delete Selected", type="secondary")

        if submit_delete:
            victims_lc = [v.strip().lower() for v in victims if v]
            me = user_email.strip().lower()

            if not victims_lc:
                st.info("No users selected.")
            elif confirm_text.strip().upper() != "DELETE":
                st.warning('Confirmation failed. Please type "DELETE" to proceed.')
            elif me in victims_lc:
                st.error("You cannot delete your own account.")
            else:
                with get_service_account_connection() as con:
                    # Last-admin guard
                    if enforce_admin_guard_del:
                        users_df = fetch_all_users(con, tenant_id)
                        by_email = {r["EMAIL"].strip().lower(): r["ROLE"].strip().upper() for _, r in users_df.iterrows()}
                        admins_selected = [e for e in victims_lc if by_email.get(e) == "ADMIN"]
                        if admins_selected:
                            current_admins = count_admins(con, tenant_id)
                            remaining = current_admins - len(admins_selected)
                            if remaining <= 0:
                                st.error("Blocked: This action would delete the last ADMIN in the tenant.")
                                st.stop()

                    deleted = delete_users(con, tenant_id, victims_lc)
                    con.commit()

                st.success(f"Deleted {deleted} user(s).")
                _invalidate_caches()
                st.rerun()



    # --- Reset Logs (form) ---
    with st.expander("🔎 Reset Logs Viewer", expanded=False):
        with st.form("reset_logs_form", clear_on_submit=False):
            cc = st.columns([1,1,1,1,1])
            with cc[0]:
                f_email = st.text_input("Filter by Email (optional)", key="reset_email_form")
            with cc[1]:
                f_success = st.selectbox("Success", ["Any", "True", "False"], index=0, key="reset_success_form")
                f_success_val = None if f_success == "Any" else f_success
            with cc[2]:
                f_from = st.date_input("From", value=(datetime.utcnow() - timedelta(days=7)).date(), key="reset_from_form")
            with cc[3]:
                f_to = st.date_input("To", value=datetime.utcnow().date(), key="reset_to_form")
            with cc[4]:
                f_limit = st.number_input("Limit", min_value=10, max_value=5000, value=500, step=50, key="reset_limit_form")
            submit_reset = st.form_submit_button("Apply")

        if submit_reset:
            dt_from_dt = datetime.combine(f_from, datetime.min.time())
            dt_to_dt = datetime.combine(f_to, datetime.max.time())
            df = _get_reset_logs(
                tenant_id,
                (f_email or "").strip() or None,
                f_success_val,
                dt_from_dt,
                dt_to_dt,
                int(f_limit),
            )
            st.session_state["reset_logs_result"] = df

        if "reset_logs_result" in st.session_state:
            st.caption("Most recent first.")
            st.dataframe(st.session_state["reset_logs_result"], width='stretch')



    # --- Failed Login Viewer (form) ---
    with st.expander("🚫 Failed Login Viewer", expanded=False):
        with st.form("failed_logs_form", clear_on_submit=False):
            cc = st.columns([1,1,1,1])
            with cc[0]:
                fl_email = st.text_input("Filter by Email (optional)", key="failed_email_form")
            with cc[1]:
                fl_from = st.date_input("From", value=(datetime.utcnow() - timedelta(days=7)).date(), key="failed_from_form")
            with cc[2]:
                fl_to = st.date_input("To", value=datetime.utcnow().date(), key="failed_to_form")
            with cc[3]:
                fl_limit = st.number_input("Limit", min_value=10, max_value=5000, value=500, step=50, key="failed_limit_form")
            submit_failed = st.form_submit_button("Apply")

        if submit_failed:
            fl_from_dt = datetime.combine(fl_from, datetime.min.time())
            fl_to_dt = datetime.combine(fl_to, datetime.max.time())
            df = _get_failed(
                tenant_id,
                (fl_email or "").strip() or None,
                fl_from_dt,
                fl_to_dt,
                int(fl_limit),
            )
            st.session_state["failed_logs_result"] = df

        if "failed_logs_result" in st.session_state:
            st.caption("Most recent failures first.")
            st.dataframe(st.session_state["failed_logs_result"], width='stretch')



    # --- Unlock User Accounts (form) ---
    with st.expander("🔓 Unlock User Accounts", expanded=False):
        with st.form("unlock_users_form", clear_on_submit=True):
            locked_df = _get_locked(tenant_id)
            locked_list = locked_df["EMAIL"].tolist() if not locked_df.empty else []
            selected = st.multiselect("Locked Users", locked_list, placeholder="Pick users to unlock…")
            select_all = st.checkbox("Select all locked users")
            reason = st.text_input("Reason (optional; logged and emailed)")
            submit_unlock = st.form_submit_button("Unlock Selected", type="primary")

        if submit_unlock:
            targets = locked_list if select_all else selected
            if not targets:
                st.info("No users selected.")
            else:
                ok_count, fail_count = 0, 0
                for email in targets:
                    ok, msg = unlock_user_account(
                        email,
                        unlocked_by=user_email,
                        tenant_id=tenant_id,
                        reason=reason or "Manual unlock",
                    )
                    if ok:
                        ok_count += 1
                    else:
                        fail_count += 1
                        st.warning(f"{email}: {msg}")
                if ok_count:
                    st.success(f"Unlocked {ok_count} account(s).")
                if fail_count:
                    st.warning(f"{fail_count} account(s) failed to unlock.")
                _invalidate_caches()
                st.rerun()

  

# Optional local run support
if __name__ == "__main__":
    render()
