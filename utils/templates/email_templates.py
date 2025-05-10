# utils/templates/email_templates.py

def reset_password_template(first_name, reset_link):
    return f"""
    <html>
      <body>
        <h3>Password Reset Requested</h3>
        <p>Hi {first_name},</p>
        <p>Click below to reset your password:</p>
        <a href="{reset_link}">{reset_link}</a>
        <p>This link is valid for 1 hour.</p>
        <p>If you didn't request this, please ignore this message.</p>
      </body>
    </html>
    """

def unlock_notification_template(first_name, unlocker_name):
    return f"""
    <html>
      <body>
        <h3>Your Account Has Been Unlocked ✅</h3>
        <p>Hi {first_name},</p>
        <p>Your account has been unlocked by {unlocker_name}.</p>
        <p>You may now <a href="http://localhost:8501">log in</a>.</p>
        <br>
        <p>If you didn't expect this change, contact support.</p>
      </body>
    </html>
    """


