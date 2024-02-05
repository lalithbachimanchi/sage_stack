import mysql.connector
from faker import Faker
from random import randint, choice
from datetime import date, timedelta

# Connect to MySQL
connection = mysql.connector.connect(
    host='your_mysql_host',
    user='your_mysql_user',
    password='your_mysql_password',
    database='your_mysql_database'
)

# Create a cursor
cursor = connection.cursor()

# Function to generate fake data
def generate_fake_data():
    fake = Faker()
    return {
        'external_account_id': fake.uuid4(),
        'original_balance': round(randint(1000, 10000) + randint(0, 99) / 100, 2),
        'origination_date': fake.date_between(start_date='-2y', end_date='today'),
        'account_status': choice(['Active', 'Closed', 'Pending']),
        # Add more fields and their data types here
    }

# Number of records to insert
num_records = 100

# Insert seed data
for _ in range(num_records):
    data = generate_fake_data()

    # SQL query to insert data into the table
    sql = """
    INSERT INTO genaidb.health_care_data (
        external_account_id, original_balance, origination_date, account_status, 
        date_last_worked, number_of_payments, date_last_paid, date_last_promise, 
        number_of_broken_promises, date_last_broken, number_of_calls, number_of_contacts, 
        date_last_contacted, number_of_letters_sent, number_of_emails_sent, 
        number_of_emails_opened, email_response, date_last_email_sent, 
        date_last_email_open, date_last_email_response, number_of_texts_sent, 
        text_response, date_last_text, date_last_text_response, web_vists, 
        payment_channel, date_of_treatment, insurance_carrier, judgement_date, 
        current_balance, is_historical, communication_preference, 
        number_of_calls_received, number_of_estatements_sent, last_payment_amount, 
        last_payment_type, financial_class, primary_insurance, secondary_insurance, 
        patient_type, collection_type, date_received, first_name, last_name, 
        social_security_number, date_of_birth, external_person_id, address1, 
        address2, city, state, zip_code, email_address, phone_number, 
        formatted_phone_number
    ) VALUES (
        %(external_account_id)s, %(original_balance)s, %(origination_date)s, 
        %(account_status)s, %(date_last_worked)s, %(number_of_payments)s, 
        %(date_last_paid)s, %(date_last_promise)s, %(number_of_broken_promises)s, 
        %(date_last_broken)s, %(number_of_calls)s, %(number_of_contacts)s, 
        %(date_last_contacted)s, %(number_of_letters_sent)s, %(number_of_emails_sent)s, 
        %(number_of_emails_opened)s, %(email_response)s, %(date_last_email_sent)s, 
        %(date_last_email_open)s, %(date_last_email_response)s, %(number_of_texts_sent)s, 
        %(text_response)s, %(date_last_text)s, %(date_last_text_response)s, %(web_vists)s, 
        %(payment_channel)s, %(date_of_treatment)s, %(insurance_carrier)s, 
        %(judgement_date)s, %(current_balance)s, %(is_historical)s, 
        %(communication_preference)s, %(number_of_calls_received)s, 
        %(number_of_estatements_sent)s, %(last_payment_amount)s, 
        %(last_payment_type)s, %(financial_class)s, %(primary_insurance)s, 
        %(secondary_insurance)s, %(patient_type)s, %(collection_type)s, 
        %(date_received)s, %(first_name)s, %(last_name)s, 
        %(social_security_number)s, %(date_of_birth)s, %(external_person_id)s, 
        %(address1)s, %(address2)s, %(city)s, %(state)s, %(zip_code)s, 
        %(email_address)s, %(phone_number)s, %(formatted_phone_number)s
    )
    """

    cursor.execute(sql, data)

# Commit changes and close connection
connection.commit()
cursor.close()
connection.close()
