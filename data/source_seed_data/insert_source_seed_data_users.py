import mysql.connector
from faker import Faker
from datetime import datetime, timedelta
import os

# Connect to MySQL
# connection = mysql.connector.connect(
#     host='localhost',
#     user='root',
#     password='root',
#     database='genaidb',
#     port=3307
# )

connection = mysql.connector.connect(
    host=os.environ['MYSQL_HOST_NAME'],
    user=os.environ['MYSQL_USER'],
    password=os.environ['MYSQL_PASSWORD'],
    database=os.environ['MYSQL_DATABASE'],
    port=os.environ['MYSQL_PORT']
)


# Function to generate fake data
def generate_fake_user_data():
    fake = Faker()
    return {
        'ID': fake.unique.random_number(digits=5),
        'user_login': fake.user_name(),
        'user_pass': fake.password(),
        'user_nicename': fake.user_name(),
        'user_email': fake.email(),
        'user_url': fake.url(),
        'user_registered': fake.date_time_this_decade(),
        'user_activation_key': fake.uuid4(),
        'user_status': fake.random_int(min=0, max=1),
        'display_name': fake.name()
    }

def generate_fake_usermeta_data(user_id):
    fake = Faker()
    return {
        'umeta_id': fake.unique.random_number(digits=5),
        'user_id': user_id,
        'meta_key': fake.word(),
        'meta_value': fake.word()
    }

# Number of records to insert
num_records = 5

user_ids = []

cursor = connection.cursor()
for _ in range(num_records):
    data = generate_fake_user_data()

    user_ids.append(data['ID'])
    # SQL query to insert data into the table
    sql = """
    INSERT INTO genaidb.users (
        ID, user_login, user_pass, user_nicename, user_email, user_url, 
        user_registered, user_activation_key, user_status, display_name
    ) VALUES (
        %(ID)s, %(user_login)s, %(user_pass)s, %(user_nicename)s, %(user_email)s, 
        %(user_url)s, %(user_registered)s, %(user_activation_key)s, 
        %(user_status)s, %(display_name)s
    )
    """

    cursor.execute(sql, data)

connection.commit()
cursor.close()

cursor = connection.cursor()
for user in user_ids:
    data = generate_fake_usermeta_data(user_id=user)

    # SQL query to insert data into the table
    sql = """
    INSERT INTO genaidb.usermeta (
        umeta_id, user_id, meta_key, meta_value
    ) VALUES (
        %(umeta_id)s, %(user_id)s, %(meta_key)s, %(meta_value)s
    )
    """

    cursor.execute(sql, data)

connection.commit()
cursor.close()


connection.close()


