####
## Email utilities
## Tech Implementation Answer by Mario Caesar // caesarmario87@gmail.com
####
from airflow.sdk import Variable

import smtplib
from email.message import EmailMessage

def send_email_alert(subject: str, body: str):
    creds = Variable.get("email_alert_creds", deserialize_json=True)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = creds["sender"]
    msg["To"] = creds["receiver"]
    msg.set_content(body)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(creds["sender"], creds["password"])
        smtp.send_message(msg)