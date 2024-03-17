from flask import Flask
from kafka import KafkaConsumer
import json
import mysql.connector
import threading

app = Flask(__name__)

# Kafka Consumer Configuration
KAFKA_TOPIC = 'health-care-data'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# MySQL Database Configuration
MYSQL_HOST = 'localhost'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'root'
MYSQL_DATABASE = 'genaidb'
MYSQL_PORT = '3307'

mysql_conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE,
    port=MYSQL_PORT
)
mysql_cursor = mysql_conn.cursor()


def insert_data_into_mysql(data):
    # Insert data into MySQL table
    insert_query = """
        INSERT INTO health_care_data (
            external_account_id, original_balance, origination_date, account_status, date_last_worked,
            number_of_payments, date_last_paid, date_last_promise, number_of_broken_promises, date_last_broken,
            number_of_calls, number_of_contacts, date_last_contacted, number_of_letters_sent, number_of_emails_sent,
            number_of_emails_opened, email_response, date_last_email_sent, date_last_email_open, date_last_email_response,
            number_of_texts_sent, text_response, date_last_text, date_last_text_response, web_vists, payment_channel,
            date_of_treatment, insurance_carrier, judgement_date, current_balance, is_historical, communication_preference,
            number_of_calls_received, number_of_estatements_sent, last_payment_amount, last_payment_type, financial_class,
            primary_insurance, secondary_insurance, patient_type, collection_type, date_received, first_name, last_name,
            social_security_number, date_of_birth, external_person_id, address1, address2, city, state, zip_code,
            email_address, phone_number
        ) VALUES (
            %(external_account_id)s, %(original_balance)s, %(origination_date)s, %(account_status)s, %(date_last_worked)s,
            %(number_of_payments)s, %(date_last_paid)s, %(date_last_promise)s, %(number_of_broken_promises)s, %(date_last_broken)s,
            %(number_of_calls)s, %(number_of_contacts)s, %(date_last_contacted)s, %(number_of_letters_sent)s, %(number_of_emails_sent)s,
            %(number_of_emails_opened)s, %(email_response)s, %(date_last_email_sent)s, %(date_last_email_open)s, %(date_last_email_response)s,
            %(number_of_texts_sent)s, %(text_response)s, %(date_last_text)s, %(date_last_text_response)s, %(web_vists)s, %(payment_channel)s,
            %(date_of_treatment)s, %(insurance_carrier)s, %(judgement_date)s, %(current_balance)s, %(is_historical)s, %(communication_preference)s,
            %(number_of_calls_received)s, %(number_of_estatements_sent)s, %(last_payment_amount)s, %(last_payment_type)s, %(financial_class)s,
            %(primary_insurance)s, %(secondary_insurance)s, %(patient_type)s, %(collection_type)s, %(date_received)s, %(first_name)s, %(last_name)s,
            %(social_security_number)s, %(date_of_birth)s, %(external_person_id)s, %(address1)s, %(address2)s, %(city)s, %(state)s, %(zip_code)s,
            %(email_address)s, %(phone_number)s
        )
    """

    mysql_cursor.execute(insert_query, data)
    mysql_conn.commit()

    # mysql_cursor.close()
    # mysql_conn.close()

    print("Data inserted into MySQL table successfully.")

def kafka_consumer():
    consumer = KafkaConsumer(KAFKA_TOPIC,
                             bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                             value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    # try:
    for message in consumer:
        print(message)
        data = message.value
        print(data)
        import pdb
        # pdb.set_trace()
        json_data = json.loads(data)
        insert_data_into_mysql(json_data)
        print("Data inserted into MySQL:", data)
    # except Exception as e:
    #     print("Error:", e)
    # finally:
    #     consumer.close()

@app.route('/')
def home():
    return 'Welcome to Kafka to MySQL microservice!'

if __name__ == '__main__':
    kafka_thread = threading.Thread(target=kafka_consumer)
    kafka_thread.daemon = True
    kafka_thread.start()

    app.run(debug=True)
