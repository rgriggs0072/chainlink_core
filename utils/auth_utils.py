# utils/auth_utils.py
# ------------------------------------------------------------------------------
# Centralized authentication & role utilities for Chainlink Core
# Updated: now uses flattened USERDATA.ROLE (ADMIN / USER)
# ------------------------------------------------------------------------------

import secrets
from datetime import datetime, timedelta, timezone
import requests
import bcrypt
from snowflake.connector import ProgrammingError
from sf_connector.service_connector import get_service_account_connection
from utils.email_utils import send_unlock_notification
from auth.forgot_password import send_reset_email


# ------------------------------------------------------------------------------
# 🌐 Get client IP (for login logging)
# ------------------------------------------------------------------------------
def get_ip_address():
    try:
        return requests.get("https://api.ipify.org").text
    except Exception:
        return "unknown"


# ------------------------------------------------------------------------------
# 🛡️ is_admin_user(email, tenant_id)
# Checks if user has ROLE = 'ADMIN' in USERDATA
# ------------------------------------------------------------------------------
def is_admin_user(user_email: str, tenant_id: str) -> bool:
    try:
        conn = get_service_account_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT ROLE
            FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
            WHERE LOWER(EMAIL) = LOWER(%s)
              AND TENANT_ID = %s
              AND IS_ACTIVE = TRUE
              AND COALESCE(IS_LOCKED, FALSE) = FALSE
            LIMIT 1
        """, (user_email, tenant_id))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row or not row[0]:
            return False
        return str(row[0]).strip().upper() == "ADMIN"
    except Exception as e:
        print(f"Role check failed: {e}")
        return False

# ------------------------------------------------------------------------------
# 🔒 is user active?
# Returns True if account is active
# ------------------------------------------------------------------------------

def get_user_status(email: str) -> tuple[bool | None, bool | None, bool]:
    """
    Returns (is_active, is_locked, exists) for EMAIL across tenants.
    Use when auth_status is False to show clearer messages and avoid incrementing attempts on disabled users.
    """
    try:
        from sf_connector.service_connector import get_service_account_connection
        conn = get_service_account_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(IS_ACTIVE, TRUE), COALESCE(IS_LOCKED, FALSE)
                FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
                WHERE LOWER(EMAIL) = LOWER(%s)
                LIMIT 1
            """, (email,))
            row = cur.fetchone()
        conn.close()
        if not row:
            return (None, None, False)
        return (bool(row[0]), bool(row[1]), True)
    except Exception:
        return (None, None, False)



def is_user_active(email: str, tenant_id: str) -> bool:
    try:
        conn = get_service_account_connection()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(IS_ACTIVE, TRUE)
                FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
                WHERE LOWER(EMAIL) = LOWER(%s) AND TENANT_ID = %s
                LIMIT 1
            """, (email, tenant_id))
            row = cur.fetchone()
        conn.close()
        return bool(row and row[0])
    except Exception:
        return False


# ------------------------------------------------------------------------------
# 🔒 is_user_locked_out(email)
# Returns True if account is currently locked
# ------------------------------------------------------------------------------
def is_user_locked_out(email: str) -> bool:
    try:
        conn = get_service_account_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT IS_LOCKED
            FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
            WHERE LOWER(EMAIL) = LOWER(%s)
        """, (email,))
        result = cur.fetchone()
        cur.close()
        conn.close()
        return bool(result and result[0])
    except Exception as e:
        print(f"Lockout check failed: {e}")
        return False


# ------------------------------------------------------------------------------
# 📉 increment_failed_attempts(email)
# Increments FAILED_ATTEMPTS, locks after 3 tries, logs IP
# ------------------------------------------------------------------------------
def increment_failed_attempts(email: str):
    try:
        conn = get_service_account_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT FAILED_ATTEMPTS, TENANT_ID
            FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
            WHERE LOWER(EMAIL) = LOWER(%s)
        """, (email,))
        row = cur.fetchone()

        if row:
            attempts = row[0] or 0
            tenant_id = row[1]
            new_attempts = attempts + 1

            if new_attempts >= 3:
                cur.execute("""
                    UPDATE TENANTUSERDB.CHAINLINK_SCH.USERDATA
                    SET FAILED_ATTEMPTS = %s, IS_LOCKED = TRUE
                    WHERE LOWER(EMAIL) = LOWER(%s)
                """, (new_attempts, email))
            else:
                cur.execute("""
                    UPDATE TENANTUSERDB.CHAINLINK_SCH.USERDATA
                    SET FAILED_ATTEMPTS = %s
                    WHERE LOWER(EMAIL) = LOWER(%s)
                """, (new_attempts, email))

            ip_address = get_ip_address()
            cur.execute("""
                INSERT INTO TENANTUSERDB.CHAINLINK_SCH.FAILED_LOGINS
                (EMAIL, TIMESTAMP, IP_ADDRESS, TENANT_ID)
                VALUES (%s, CURRENT_TIMESTAMP, %s, %s)
            """, (email, ip_address, tenant_id))

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Failed to increment attempts: {e}")


# ------------------------------------------------------------------------------
# 🔓 reset_failed_attempts(email)
# Clears failed login count + unlocks account
# ------------------------------------------------------------------------------
def reset_failed_attempts(email: str):
    try:
        conn = get_service_account_connection()
        cur = conn.cursor()
        cur.execute("""
            UPDATE TENANTUSERDB.CHAINLINK_SCH.USERDATA
            SET FAILED_ATTEMPTS = 0, IS_LOCKED = FALSE
            WHERE LOWER(EMAIL) = LOWER(%s)
        """, (email,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Failed to reset failed attempts: {e}")


# ------------------------------------------------------------------------------
# 🔓 unlock_user_account(email)
# Admin-level manual unlock; logs event + sends notification
# ------------------------------------------------------------------------------
def unlock_user_account(email, unlocked_by=None, tenant_id=None, reason="Manual unlock"):
    try:
        conn = get_service_account_connection()
        cur = conn.cursor()

        # Step 1: Unlock user
        cur.execute("""
            UPDATE TENANTUSERDB.CHAINLINK_SCH.USERDATA
            SET FAILED_ATTEMPTS = 0, IS_LOCKED = FALSE
            WHERE LOWER(EMAIL) = LOWER(%s)
        """, (email,))

        # Step 2: Log unlock
        if unlocked_by and tenant_id:
            cur.execute("""
                INSERT INTO TENANTUSERDB.CHAINLINK_SCH.UNLOCK_LOGS
                (UNLOCKED_EMAIL, UNLOCKED_BY, TENANT_ID, REASON)
                VALUES (%s, %s, %s, %s)
            """, (email, unlocked_by, tenant_id, reason))

        # Step 3: Get first name for email
        cur.execute("""
            SELECT FIRST_NAME
            FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
            WHERE LOWER(EMAIL) = LOWER(%s)
        """, (email,))
        row = cur.fetchone()
        user_first_name = row[0] if row else "User"

        conn.commit()
        cur.close()
        conn.close()

        # Step 4: Send unlock email
        send_unlock_notification(email, first_name=user_first_name, unlocker_name=unlocked_by)

        return True, "User unlocked successfully."

    except Exception as e:
        return False, f"Error unlocking user: {e}"


# ------------------------------------------------------------------------------
# 👤 create_user_account(conn, ...)
# Creates a new user and sends reset/invite email
# ------------------------------------------------------------------------------
def create_user_account(conn, email, first_name, last_name, role_name, tenant_id):
    try:
        cur = conn.cursor()

        # Prevent duplicates
        cur.execute("""
            SELECT 1 FROM USERDATA
            WHERE LOWER(EMAIL) = LOWER(%s) AND TENANT_ID = %s
        """, (email, tenant_id))
        if cur.fetchone():
            return False, "Email already exists."

        # Generate new ID + token
        next_id = cur.execute("SELECT COALESCE(MAX(USER_ID), 0) + 1 FROM USERDATA").fetchone()[0]
        token = secrets.token_urlsafe()
        expiry = datetime.now(timezone.utc) + timedelta(hours=1)

        # Insert user
        cur.execute("""
            INSERT INTO USERDATA (
                USER_ID, HASHED_PASSWORD, EMAIL, TENANT_ID,
                FIRST_NAME, LAST_NAME, ROLE, IS_ACTIVE,
                FAILED_ATTEMPTS, IS_LOCKED,
                RESET_TOKEN, TOKEN_EXPIRY
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE, 0, FALSE, %s, %s)
        """, (
            next_id, None, email, tenant_id,
            first_name, last_name, role_name.strip().upper(),
            token, expiry
        ))

        conn.commit()
        cur.close()

        # Send reset email
        send_reset_email(email, token)
        return True, "✅ User created and invitation email sent."

    except Exception as e:
        return False, f"❌ Failed to create user: {e}"
