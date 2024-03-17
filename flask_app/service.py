from flask import Flask
from kafka import KafkaConsumer
import json
import mysql.connector
import schedule
import time

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

def consume_and_insert():
    print("Consuming records from Kafka and Transforming")
    try:
        for message in consumer:
            print(message)
            data = message.value
            insert_data_into_mysql(data)
            print("Data inserted into MySQL:", data)
    except Exception as e:
        print("Error:", e)

def insert_data_into_mysql(data):
    # Insert data into MySQL table
    insert_query = "INSERT INTO your_table (column1, column2) VALUES (%s, %s)"
    values = (data['column1'], data['column2'])  # Modify according to your JSON structure

    mysql_cursor.execute(insert_query, values)
    mysql_conn.commit()

@app.route('/home')
def home():
    return 'Home'

# Flask Route to start consuming and inserting data
@app.route('/start-consuming-and-inserting')
def start_consuming_and_inserting():
    schedule.every(5).seconds.do(consume_and_insert)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    app.run(debug=True)
