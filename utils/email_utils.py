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

def send_reset_email(email, token, first_name="User"):
    sender_email = "randy@chainlinkanalytics.com"
    reset_link = f"http://localhost:8501/reset_password?token={token}"

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

