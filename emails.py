import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.base import MIMEBase
import os
from datetime import datetime as dt

def send_email(email_to, subject = "", html_text = "", attachments=[]):
    
    email_from = "ESO.robotas@eso.lt"
    msg = MIMEMultipart()
    msg["Subject"] = "RPA proceso stebesena {1} {0:%Y-%m-%d %H:%M}".format(dt.now(), subject)
    msg["From"] = email_from
    msg["To"] = ", ".join(email_to) if isinstance(email_to, list) else email_to
    
    if attachments:
        for atach in attachments:
            with open(atach, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())
            
            encoders.encode_base64(part)
            part.add_header(
            "Content-Disposition",
            "attachment", filename=(os.path.basename(atach)),
            )
            
            msg.attach(part)
    
    part_html = MIMEText(html_text, "html")

    msg.attach(part_html)

    
    smtp_server = smtplib.SMTP("VTIC-SMTP.corp.rst.lt", 25)
    smtp_server.sendmail(email_from, email_to, msg.as_string())
    smtp_server.quit()

if __name__ == "__main__":    
    send_email(["dainius.mieziunas@eso.lt"], html_text="""<h1>Test text</h1> <h1>Test text2</h1>""")
    send_email(["dainius.mieziunas@eso.lt"], 
                            "self.process_name", 
                            f"Pastebeta daug klaidu ")