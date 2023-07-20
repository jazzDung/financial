from financial.resources import EMAIL_SENDER, EMAIL_PASSWORD, DB_CONNECTION, SMTP
from email.message import EmailMessage
from dagster import asset
from datetime import datetime

@asset(group_name="email")
def fetch_unchecked():
    """
    Find unchecked records
    """
    output = DB_CONNECTION.execute(
        """
        SELECT * 
        FROM financial_clean.user_query 
        WHERE checked = False;
        """)
    result = output.fetchall()
    return [record._asdict() for record in result]

@asset(group_name="email")
def send_email(fetch_unchecked):
    """
    Send email to user with unchecked records
    """
    SMTP.login(EMAIL_SENDER, EMAIL_PASSWORD)
    
    # TODO: Implement try except for when email is invalid
    for record in fetch_unchecked:
        body = record['mail_subject'] or "BLANK mail_subject"
        receiver = record['email']
        subject = 'Test Email'

        em = EmailMessage()
        em['From'] = EMAIL_SENDER
        em['To'] = receiver
        em['Subject'] = subject
        em.set_content(body)

        SMTP.sendmail(EMAIL_SENDER, receiver, em.as_string())

    SMTP.quit()
    return fetch_unchecked

@asset(group_name="email")
def check_record(send_email):
    """
    Alter records after sending email
    """
    for record in send_email:
        id = record['id']
        now = str(datetime.now())[:-7]+"+07"

        DB_CONNECTION.execute(
            f"""
            UPDATE financial_clean.user_query
            SET 
                checked = True, 
                last_checked = '{now}'
            WHERE id = {id};
            """)
