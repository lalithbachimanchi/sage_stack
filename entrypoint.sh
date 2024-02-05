#!/bin/bash

# Initialize the Airflow database

pip freeze
airflow version

sleep 5

airflow db init

sleep 5

airflow users create \
-e admin@sampleadmin.com \
-f Admin \
-l User \
-p password \
-r Admin \
-u admin

sleep 3

# Start the Airflow scheduler
airflow scheduler &

sleep 10

# Start the Airflow webserver
airflow webserver
