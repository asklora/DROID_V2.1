import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

USERNAME = "asklora@loratechai.com"
PASSWORD = "askLORA20$"

mail_content = '''Hello,
This is a simple mail. There is only text, no attachments are there The mail is sent using Python SMTP library.
Thank You'''
send_from = 'asklora@loratechai.com'
send_to = ['stepchoi@loratechai.com', 'info@loratechai.com', 'imansaj@loratechai.com']
subject = 'A test mail sent by Python. It has an attachment.'
msg = MIMEMultipart()
msg['From'] = send_from
x = COMMASPACE.join(send_to)
msg['To'] = COMMASPACE.join(send_to)
msg['Date'] = formatdate(localtime=True)
msg['Subject'] = subject
msg.attach(MIMEText(mail_content, 'plain'))
context = ssl.create_default_context()

try:
    server = smtplib.SMTP('smtp.gmail.com', 587)
    # server = smtplib.SMTP_SSL('smtp.gmail.com', 465) # if
    server.ehlo()
    server.starttls(context=context)
    server.login(USERNAME, PASSWORD)
    server.sendmail(send_from, send_to, msg.as_string())  # need as_string
    server.close()

    print('Email sent!')
except Exception as e:
    print('Something went wrong...')
    print(e)
