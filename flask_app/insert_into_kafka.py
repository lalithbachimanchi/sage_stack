import csv
import json
from kafka import KafkaProducer
import time

# Kafka configurations
bootstrap_servers = 'localhost:9092'
topic_name = 'health-care-data'

# Function to read CSV file and convert each row into JSON
def csv_to_json(csv_file):
    json_data = []
    with open(csv_file, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            json_data.append(json.dumps(row))
    return json_data

# Function to send JSON data to Kafka queue
def send_to_kafka(json_data):
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    for data in json_data:
        producer.send(topic_name, value=data)

    producer.flush()
    producer.close()

if __name__ == '__main__':
    csv_file = '/Users/lalith/GenAIPoc/sage_stack/data/sample_etl_data.csv'  # Path to your CSV file
    while True:
        json_data = csv_to_json(csv_file)
        send_to_kafka(json_data)
        print("Data sent to Kafka queue successfully!")
        time.sleep(2)  # Wait for 3 seconds before sending data again
