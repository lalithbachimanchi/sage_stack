import csv
import json
from kafka import KafkaProducer
import time

# Kafka configurations
bootstrap_servers = 'localhost:9092'
topic_name = 'health-care-data'


# Function to send JSON data to Kafka queue
def send_to_kafka():
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    data = {"external_account_id": "82087", "original_balance": "4965", "origination_date": "2021-03-17", "account_status": "Inactived", "date_last_worked": "2023-05-29", "number_of_payments": "99", "date_last_paid": "2023-04-18", "date_last_promise": "2022-03-01", "number_of_broken_promises": "86", "date_last_broken": "2019-03-03", "number_of_calls": "856", "number_of_contacts": "4", "date_last_contacted": "2020-04-09", "number_of_letters_sent": "7", "number_of_emails_sent": "101", "number_of_emails_opened": "92", "email_response": "No", "date_last_email_sent": "2020-04-06", "date_last_email_open": "2021-11-24", "date_last_email_response": "2021-07-10", "number_of_texts_sent": "54", "text_response": "Yedss", "date_last_text": "2019-05-23", "date_last_text_response": "2023-01-06", "web_vists": "81", "payment_channel": "Cheque", "date_of_treatment": "2023-08-13", "insurance_carrier": "Smith Inc", "judgement_date": "2023-06-27", "current_balance": "49965", "is_historical": "No", "communication_preference": "Email", "number_of_calls_received": "54", "number_of_estatements_sent": "97", "last_payment_amount": "9085", "last_payment_type": "Credit Card", "financial_class": "C", "primary_insurance": "Sanchez LLC", "secondary_insurance": "Nelson-Mcdowell", "patient_type": "Inpatient", "collection_type": "External", "date_received": "2020-10-27", "first_name": "Alexandra", "last_name": "Ball", "social_security_number": "513-57-0917", "date_of_birth": "1986-12-25", "external_person_id": "7916", "address1": "599 Tanya Pike Apt. 100", "address2": "Suite 073", "city": "Lake Haley", "state": "NH", "zip_code": "54464", "email_address": "matthew86@example.net", "phone_number": "1124070683"}

    producer.send(topic_name, value=data)

    producer.flush()
    producer.close()

if __name__ == '__main__':
    csv_file = '/Users/lalith/GenAIPoc/sage_stack/data/sample_etl_data.csv'  # Path to your CSV file
    send_to_kafka()
    print("Data sent to Kafka queue successfully!")
