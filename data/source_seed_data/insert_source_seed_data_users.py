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
        'user_id': fake.unique.random_number(digits=5),
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

def generate_fake_post_data(user_id):
    fake = Faker()
    return {
        'post_id': fake.unique.random_number(digits=5),
        'post_author': user_id,
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

def generate_fake_order_data(user_id):
    fake = Faker()
    return {
        'order_item_id': fake.unique.random_number(digits=5),
        'order_item_name': fake.word(),
        'order_item_type': fake.word(),
        'order_id': fake.unique.random_number(digits=5),
        'order_user': user_id
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
num_records = 50

user_ids = []
post_ids = []
order_items_ids = []

cursor = connection.cursor()
for _ in range(num_records):
    data = generate_fake_user_data()

    user_ids.append(data['user_id'])
    # SQL query to insert data into the table
    sql = """
    INSERT INTO genaidb.users (
        user_id, user_login, user_pass, user_nicename, user_email, user_url, 
        user_registered, user_activation_key, user_status, display_name
    ) VALUES (
        %(user_id)s, %(user_login)s, %(user_pass)s, %(user_nicename)s, %(user_email)s, 
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


cursor = connection.cursor()
for user in user_ids:
    data = generate_fake_post_data(user)

    post_ids.append(data['post_id'])

    # SQL query to insert data into the table
    sql = """
    INSERT INTO genaidb.posts (
        post_id, post_author, post_date, post_date_gmt, post_content, 
        post_title, post_excerpt, post_status, comment_status, ping_status, 
        post_password, post_name, to_ping, pinged, post_modified, 
        post_modified_gmt, post_content_filtered, post_parent, guid, 
        menu_order, post_type, post_mime_type, comment_count
    ) VALUES (
        %(post_id)s, %(post_author)s, %(post_date)s, %(post_date_gmt)s, 
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

connection.commit()
cursor.close()


cursor = connection.cursor()
for user in user_ids:
    data = generate_fake_order_data(user)

    order_items_ids.append(data['order_item_id'])

    # SQL query to insert data into the table
    sql = """
        INSERT INTO genaidb.commerce_order_items (
            order_item_id, order_item_name, order_item_type, order_id, order_user
        ) VALUES (
            %(order_item_id)s, %(order_item_name)s, %(order_item_type)s, %(order_id)s, %(order_user)s
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

connection.commit()
cursor.close()

connection.close()


