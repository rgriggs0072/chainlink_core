import streamlit as st
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from utils.templates import email_templates

def get_mailjet_server():
    creds = st.secrets["mailjet"]
    server = smtplib.SMTP("in-v3.mailjet.com", 587)
    server.starttls()
    server.login(creds["API_KEY"], creds["SECRET_KEY"])
    return server


def send_reset_email(email: str, token: str, first_name: str = "User") -> None:
    """
    Send a password reset email with a time-limited token.

    - Uses the deployed Streamlit base URL from st.secrets["app"]["base_url"].
    - Builds a reset link that hits the /reset_password route with ?token=...
    """
    sender_email = "randy@chainlinkanalytics.com"

    # Get base URL of the deployed app, e.g. "https://chainlinkcore-main.streamlit.app"
    app_base_url = st.secrets["app"]["base_url"].rstrip("/")

    # If your reset page is a dedicated /reset_password route (Streamlit page file),
    # this is correct:
    reset_link = f"{app_base_url}/reset_password?token={token}"

    # If instead your router expects something like /?page=reset_password&token=...
    # then use this line instead:
    # reset_link = f"{app_base_url}/?page=reset_password&token={token}"

    subject = "Chainlink Core – Password Reset"
    body = f"""
Hi {first_name},

We received a request to reset your Chainlink Core password.

Click the link below to set a new password (this link may expire after a short time):

{reset_link}

If you did not request this, you can safely ignore this email.

– Chainlink Analytics
"""

    # TODO: plug this back into your Mailjet / SMTP sender
    # send_email_via_mailjet(sender_email, email, subject, body)


    subject = "Chainlink Analytics Password Reset"
    html = email_templates.reset_password_template(first_name, reset_link)
    send_email(email, subject, html, sender_email)

def send_unlock_notification(email, first_name="User", unlocker_name="an admin"):
    sender_email = "randy@chainlinkanalytics.com"
    subject = "Your Chainlink Account Has Been Unlocked"
    html = email_templates.unlock_notification_template(first_name, unlocker_name)
    send_email(email, subject, html, sender_email)

def send_email(to_email, subject, html, from_email):
    msg = MIMEMultipart()
    msg["From"] = from_email
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(html, "html"))

    try:
        server = get_mailjet_server()
        server.send_message(msg)
        server.quit()
    except Exception as e:
        st.warning(f"⚠️ Failed to send email to {to_email}")
        st.exception(e)

