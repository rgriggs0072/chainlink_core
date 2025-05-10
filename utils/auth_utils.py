# utils/auth_utils.py

import secrets
from datetime import datetime
import snowflake.connector
from  snowflake.connector import ProgrammingError
from utils.email_utils import send_unlock_notification
from datetime import datetime, timedelta, timezone
from sf_connector.service_connector import get_service_account_connection
from auth.forgot_password import send_reset_email

import bcrypt

import requests

def get_ip_address():
    try:
        return requests.get("https://api.ipify.org").text
    except:
        return "unknown"


# ─────────────────────────────────────────────────────────────────────────────
# 🛡️ is_admin_user(email, tenant_id)
# Checks if the given user has an admin role (ROLE_ID = 1001)
# Returns True if user is an admin, False otherwise.
# ─────────────────────────────────────────────────────────────────────────────
def is_admin_user(user_email, tenant_id):
    try:
        conn = get_service_account_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT ur.ROLE_ID
            FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA u
            JOIN TENANTUSERDB.CHAINLINK_SCH.USER_ROLES ur ON u.USER_ID = ur.USER_ID
            WHERE u.EMAIL = %s AND u.TENANT_ID = %s
        """, (user_email, tenant_id))
        roles = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return 1001 in roles
    except Exception as e:
        print(f"Role check failed: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
# 🔒 is_user_locked_out(email)
# Checks if a user account is locked due to too many failed login attempts.
# Returns True if locked out, False otherwise.
# ─────────────────────────────────────────────────────────────────────────────
def is_user_locked_out(email):
    try:
        conn = get_service_account_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT FAILED_ATTEMPTS, LOCKOUT_TIME
            FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
            WHERE EMAIL = %s
        """, (email,))
        result = cur.fetchone()
        cur.close()
        conn.close()

        if result:
            attempts, lockout_time = result
            if attempts >= 2:
                if lockout_time is None:
                    return True
                return datetime.utcnow() < lockout_time + timedelta(minutes=15)
        return False
    except Exception as e:
        print(f"Lockout check failed: {e}")
        return False

# ─────────────────────────────────────────────────────────────────────────────
# 📉 increment_failed_attempts(email)
# Increases the FAILED_ATTEMPTS count for a user by 1.
# Locks the account if 5 or more failed attempts are reached.
# ─────────────────────────────────────────────────────────────────────────────


def increment_failed_attempts(email):
    try:
        conn = get_service_account_connection()
        cur = conn.cursor()

        # Get current failed attempts + tenant_id
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

            # Update USERDATA
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

            # Log to FAILED_LOGINS
            ip_address = get_ip_address()
            cur.execute("""
                INSERT INTO TENANTUSERDB.CHAINLINK_SCH.FAILED_LOGINS (EMAIL, TIMESTAMP, IP_ADDRESS, TENANT_ID)
                VALUES (%s, CURRENT_TIMESTAMP, %s, %s)
            """, (email, ip_address, tenant_id))

        conn.commit()
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Failed to increment attempts: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# 🔓 reset_failed_attempts(email)
# Resets a user’s FAILED_ATTEMPTS to 0 and clears the LOCKOUT_TIME.
# Usually called after a successful login.
# ─────────────────────────────────────────────────────────────────────────────
def reset_failed_attempts(email):
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

# ─────────────────────────────────────────────────────────────────────────────
# 🔓 unlock_user_account(email)
# Admin-level function to manually unlock a user account.
# Resets FAILED_ATTEMPTS and clears LOCKOUT_TIME.
# Returns (True, "message") if successful or (False, "error") on failure.
# ─────────────────────────────────────────────────────────────────────────────
def unlock_user_account(email, unlocked_by=None, tenant_id=None, reason="Manual unlock"):
    try:
        conn = get_service_account_connection()
        cursor = conn.cursor()

        # ✅ Step 1: Unlock the user
        cursor.execute("""
            UPDATE TENANTUSERDB.CHAINLINK_SCH.USERDATA
            SET FAILED_ATTEMPTS = 0, IS_LOCKED = FALSE
            WHERE LOWER(EMAIL) = LOWER(%s)
        """, (email,))

        # ✅ Step 2: Log the unlock
        if unlocked_by and tenant_id:
            cursor.execute("""
                INSERT INTO TENANTUSERDB.CHAINLINK_SCH.UNLOCK_LOGS (
                    UNLOCKED_EMAIL, UNLOCKED_BY, TENANT_ID, REASON
                )
                VALUES (%s, %s, %s, %s)
            """, (email, unlocked_by, tenant_id, reason))

        # ✅ Step 3: Get user's first name for email
        cursor.execute("""
            SELECT FIRST_NAME FROM TENANTUSERDB.CHAINLINK_SCH.USERDATA
            WHERE LOWER(EMAIL) = LOWER(%s)
        """, (email,))
        row = cursor.fetchone()
        user_first_name = row[0] if row else "User"

        conn.commit()
        cursor.close()
        conn.close()

        # ✅ Step 4: Send unlock email
        send_unlock_notification(email, first_name=user_first_name, unlocker_name=unlocked_by)

        return True, "User unlocked."

    except Exception as e:
        return False, f"Error unlocking user: {e}"

# ------ Section new user account ------

def create_user_account(conn, email, first_name, last_name, role_name, tenant_id):
    try:
        cursor = conn.cursor()

        # Check for duplicate (email + tenant_id)
        cursor.execute("""
            SELECT 1 FROM USERDATA
            WHERE LOWER(EMAIL) = LOWER(%s) AND TENANT_ID = %s
        """, (email, tenant_id))
        dup_check = cursor.fetchone()

        if dup_check:
            return False, "Email already exists."

        # Create new user
        next_id = cursor.execute("""
            SELECT COALESCE(MAX(USER_ID), 0) + 1 FROM USERDATA
        """).fetchone()[0]

        token = secrets.token_urlsafe()
        expiry = datetime.now(timezone.utc) + timedelta(hours=1)

        insert_sql = """
            INSERT INTO USERDATA (
                USER_ID, HASHED_PASSWORD, EMAIL, TENANT_ID,
                FIRST_NAME, LAST_NAME, IS_ACTIVE,
                FAILED_ATTEMPTS, LOCKOUT_TIME,
                RESET_TOKEN, TOKEN_EXPIRY
            )
            VALUES (%s, %s, %s, %s, %s, %s, TRUE, 0, NULL, %s, %s)
        """
        cursor.execute(insert_sql, (
            next_id, None, email, tenant_id,
            first_name, last_name,
            token, expiry
        ))

        send_reset_email(email, token)
        return True, "✅ User created and invitation email sent."

    except Exception as e:
        return False, f"❌ Failed to create user: {e}"



def is_user_locked_out(email):
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
        return result and result[0] == True
    except Exception as e:
        print(f"Lockout check failed: {e}")
        return False




