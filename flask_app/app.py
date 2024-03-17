from flask import Flask
from kafka import KafkaConsumer
import json
import mysql.connector

app = Flask(__name__)

# Kafka Consumer Configuration
KAFKA_TOPIC = 'health_care_data'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

consumer = KafkaConsumer(KAFKA_TOPIC,
                         bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

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


@app.route('/home')
def home():
    return 'Home'


should_run_consumer = True

@app.route('/consume-and-insert')
def consume_and_insert():
    global should_run_consumer
    empty_count = 0
    while should_run_consumer:
        print("Consuming records from Kafka and Transforming")
        try:
            for message in consumer:
                print(message)
                data = message.value
                insert_data_into_mysql(data)
                print("Data inserted into MySQL:", data)
                empty_count = 0  # Reset empty count as there was data in this iteration
        except Exception as e:
            print("Error:", e)
            continue

        empty_count += 1  # Increment empty count if no data was consumed
        if empty_count >= 3:  # Assuming no data for three consecutive iterations indicates Kafka queue is empty
            print("No records on Kafka queue. Stopping Kafka consumer.")
            stop_consumer()

def stop_consumer():
    global should_run_consumer
    should_run_consumer = False
    consumer.close()

# Flask Route to consume and insert data

# def consume_and_insert():
#     print("Consuming records from Kafka and Transforming")
#     try:
#         for message in consumer:
#             print(message)
#             data = message.value
#             # insert_data_into_mysql(data)
#             print("Data inserted into MySQL:", data)
#     except Exception as e:
#         print("Error:", e)
#         return str(e), 500
#
#     return "Data consumed and inserted into MySQL successfully"

def insert_data_into_mysql(data):
    # Insert data into MySQL table
    insert_query = "INSERT INTO your_table (column1, column2) VALUES (%s, %s)"
    values = (data['column1'], data['column2'])  # Modify according to your JSON structure

    mysql_cursor.execute(insert_query, values)
    mysql_conn.commit()

if __name__ == '__main__':
    app.run(debug=True)
