CREATE SCHEMA IF NOT EXISTS genaidb;

GRANT ALL PRIVILEGES ON DATABASE genaidb TO postgres_user;

CREATE TABLE genaidb.health_care_data (
    external_account_id VARCHAR(255),
    original_balance NUMERIC(10, 2),
    origination_date DATE,
    account_status VARCHAR(255),
    date_last_worked DATE,
    number_of_payments INT,
    date_last_paid DATE,
    date_last_promise DATE,
    number_of_broken_promises INT,
    date_last_broken DATE,
    number_of_calls INT,
    number_of_contacts INT,
    date_last_contacted DATE,
    number_of_letters_sent INT,
    number_of_emails_sent INT,
    number_of_emails_opened INT,
    email_response VARCHAR(255),
    date_last_email_sent DATE,
    date_last_email_open DATE,
    date_last_email_response DATE,
    number_of_texts_sent INT,
    text_response VARCHAR(255),
    date_last_text DATE,
    date_last_text_response DATE,
    web_vists INT,
    payment_channel VARCHAR(255),
    date_of_treatment DATE,
    insurance_carrier VARCHAR(255),
    judgement_date DATE,
    current_balance NUMERIC(10, 2),
    is_historical BOOLEAN,
    communication_preference VARCHAR(255),
    number_of_calls_received INT,
    number_of_estatements_sent INT,
    last_payment_amount NUMERIC(10, 2),
    last_payment_type VARCHAR(255),
    financial_class VARCHAR(255),
    primary_insurance VARCHAR(255),
    secondary_insurance VARCHAR(255),
    patient_type VARCHAR(255),
    collection_type VARCHAR(255),
    date_received DATE,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    social_security_number VARCHAR(255),
    date_of_birth DATE,
    external_person_id VARCHAR(255),
    address1 VARCHAR(255),
    address2 VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    zip_code VARCHAR(20),
    email_address VARCHAR(255),
    phone_number VARCHAR(20),
    formatted_phone_number VARCHAR(20),
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE genaidb.users (
    user_id VARCHAR(255),
    user_login VARCHAR(255),
    user_pass VARCHAR(255),
    user_nicename VARCHAR(255),
    user_email VARCHAR(100),
    user_url VARCHAR(255),
    user_registered TIMESTAMP,
    user_activation_key VARCHAR(255),
    user_status INT,
    display_name VARCHAR(255),
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE genaidb.usermeta (
    umeta_id VARCHAR(255),
    user_id VARCHAR(255),
    meta_key VARCHAR(255),
    meta_value VARCHAR(255),
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE genaidb.posts (
    post_id VARCHAR(255),
    post_author VARCHAR(255),
    post_date TIMESTAMP,
    post_date_gmt TIMESTAMP,
    post_content TEXT,
    post_title VARCHAR(255),
    post_excerpt TEXT,
    post_status VARCHAR(20),
    comment_status VARCHAR(20),
    ping_status VARCHAR(20),
    post_password VARCHAR(255),
    post_name VARCHAR(200),
    to_ping TEXT,
    pinged TEXT,
    post_modified TIMESTAMP,
    post_modified_gmt TIMESTAMP,
    post_content_filtered TEXT,
    post_parent INT,
    guid VARCHAR(255),
    menu_order INT,
    post_type VARCHAR(20),
    post_mime_type VARCHAR(100),
    comment_count INT,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE genaidb.postmeta (
    meta_id VARCHAR(255),
    post_id VARCHAR(255),
    meta_key VARCHAR(255),
    meta_value TEXT,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE genaidb.commerce_order_items (
    order_item_id VARCHAR(255),
    order_item_name VARCHAR(255),
    order_item_type VARCHAR(255),
    order_id VARCHAR(255),
    order_user VARCHAR(255),
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE genaidb.commerce_order_itemsmeta (
    meta_id VARCHAR(255),
    order_item_id VARCHAR(255),
    meta_key VARCHAR(255),
    meta_value TEXT,
    created TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE genaidb.sales_view (
    -- Columns from the users table
    user_id VARCHAR(255),
    user_email VARCHAR(100),
    user_url VARCHAR(255),
    user_status INT,
    display_name VARCHAR(255),

    -- Columns from the posts table
    post_id VARCHAR(255),
    post_author VARCHAR(255),
    post_date TIMESTAMP,
    post_title VARCHAR(255),
    post_excerpt TEXT,
    post_status VARCHAR(20),
    post_name VARCHAR(200),
    post_content_filtered TEXT,
    post_type VARCHAR(20),

    -- Columns from the commerce_order_items table
    order_item_id VARCHAR(255),
    order_item_name VARCHAR(255),
    order_item_type VARCHAR(255),
    order_id VARCHAR(255),

    test_email boolean,
    subscription_period VARCHAR(100),
    qualify_rn boolean,
    source_system varchar(255),
    product_segment varchar(255)
);
