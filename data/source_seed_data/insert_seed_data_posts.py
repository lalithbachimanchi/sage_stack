import mysql.connector
from faker import Faker
from datetime import datetime, timedelta
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
def generate_fake_post_data():
    fake = Faker()
    return {
        'ID': fake.unique.random_number(digits=5),
        'post_author': fake.unique.random_number(digits=5),
        'post_date': fake.date_time_this_decade(),
        'post_date_gmt': fake.date_time_this_decade(),
        'post_content': fake.text(),
        'post_title': fake.sentence(),
        'post_excerpt': fake.paragraph(),
        'post_status': fake.random_element(elements=('publish', 'draft', 'pending')),
        'comment_status': fake.random_element(elements=('open', 'closed')),
        'ping_status': fake.random_element(elements=('open', 'closed')),
        'post_password': fake.password(),
        'post_name': fake.slug(),
        'to_ping': fake.text(),
        'pinged': fake.text(),
        'post_modified': fake.date_time_this_decade(),
        'post_modified_gmt': fake.date_time_this_decade(),
        'post_content_filtered': fake.text(),
        'post_parent': fake.unique.random_number(digits=5),
        'guid': fake.uri(),
        'menu_order': fake.random_number(digits=3),
        'post_type': fake.random_element(elements=('post', 'page')),
        'post_mime_type': fake.mime_type(),
        'comment_count': fake.random_number(digits=3)
    }


def generate_fake_postmeta_data(post_id):
    fake = Faker()
    return {
        'meta_id': fake.unique.random_number(digits=5),
        'post_id': post_id,
        'meta_key': fake.word(),
        'meta_value': fake.text()
    }

# Number of records to insert
num_records = 5

post_ids = []

# Insert seed data
for _ in range(num_records):
    data = generate_fake_post_data()

    post_ids.append(data['ID'])

    # SQL query to insert data into the table
    sql = """
    INSERT INTO genaidb.posts (
        ID, post_author, post_date, post_date_gmt, post_content, 
        post_title, post_excerpt, post_status, comment_status, ping_status, 
        post_password, post_name, to_ping, pinged, post_modified, 
        post_modified_gmt, post_content_filtered, post_parent, guid, 
        menu_order, post_type, post_mime_type, comment_count
    ) VALUES (
        %(ID)s, %(post_author)s, %(post_date)s, %(post_date_gmt)s, 
        %(post_content)s, %(post_title)s, %(post_excerpt)s, %(post_status)s, 
        %(comment_status)s, %(ping_status)s, %(post_password)s, 
        %(post_name)s, %(to_ping)s, %(pinged)s, %(post_modified)s, 
        %(post_modified_gmt)s, %(post_content_filtered)s, %(post_parent)s, 
        %(guid)s, %(menu_order)s, %(post_type)s, %(post_mime_type)s, 
        %(comment_count)s
    )
    """

    cursor.execute(sql, data)

for post in post_ids:
    data = generate_fake_postmeta_data(post_id=post)

    # SQL query to insert data into the table
    sql = """
    INSERT INTO genaidb.postmeta (
        meta_id, post_id, meta_key, meta_value
    ) VALUES (
        %(meta_id)s, %(post_id)s, %(meta_key)s, %(meta_value)s
    )
    """

    cursor.execute(sql, data)

# Commit changes and close connection
connection.commit()
cursor.close()
connection.close()
