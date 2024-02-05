import mysql.connector
from faker import Faker
import os

connection = mysql.connector.connect(
    host=os.environ['MYSQL_HOST_NAME'],
    user=os.environ['MYSQL_USER'],
    password=os.environ['MYSQL_PASSWORD'],
    database=os.environ['MYSQL_DATABASE'],
    port=os.environ['MYSQL_PORT']
)

# Create a cursor
cursor = connection.cursor()

# Function to generate fake data
def generate_fake_order_data():
    fake = Faker()
    return {
        'order_item_id': fake.unique.random_number(digits=5),
        'order_item_name': fake.word(),
        'order_item_type': fake.word(),
        'order_id': fake.unique.random_number(digits=5)
    }


def generate_fake_ordermeta_data(order_item_id):
    fake = Faker()
    return {
        'meta_id': fake.unique.random_number(digits=5),
        'order_item_id': order_item_id,
        'meta_key': fake.word(),
        'meta_value': fake.text()
    }

# Number of records to insert
num_records = 5

order_items_ids = []

# Insert seed data
for _ in range(num_records):
    data = generate_fake_order_data()

    order_items_ids.append(data['order_item_id'])

    # SQL query to insert data into the table
    sql = """
    INSERT INTO genaidb.commerce_order_items (
        order_item_id, order_item_name, order_item_type, order_id
    ) VALUES (
        %(order_item_id)s, %(order_item_name)s, %(order_item_type)s, %(order_id)s
    )
    """

    cursor.execute(sql, data)


for order in order_items_ids:
    data = generate_fake_ordermeta_data(order_item_id=order)

    # SQL query to insert data into the table
    sql = """
        INSERT INTO genaidb.commerce_order_itemsmeta (
            meta_id, order_item_id, meta_key, meta_value
        ) VALUES (
            %(meta_id)s, %(order_item_id)s, %(meta_key)s, %(meta_value)s
        )
        """

    cursor.execute(sql, data)


# Commit changes and close connection
connection.commit()
cursor.close()
connection.close()
