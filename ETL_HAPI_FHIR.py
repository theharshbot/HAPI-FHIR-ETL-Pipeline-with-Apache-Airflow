from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
#from sqlalchemy import create_engine

import psycopg2

initial_url = 'http://hapi.fhir.org/baseR4/Patient?_count=20'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'HAPI_FHIR_ETL',
    description='A ETL process for FHIR',
    default_args=default_args,
    schedule_interval='@daily'
)

def extract_fn(http_client, initial_url):
    all_bundles = []
    url = initial_url

    while url:
        response = http_client.get(url)
        bundle = response.json()  # Assuming the response is in JSON format
        all_bundles.append(bundle)
        
        # Check if there's a "next" link for pagination
        next_link = next((link for link in bundle.get('link', []) if link['relation'] == 'next'), None)
        url = next_link['url'] if next_link else None  # If no next page, exit loop

    return all_bundles

def transform_fn(ti):
    all_bundles = ti.xcom_pull(task_ids='extract_task')
    patients_data = []

    for bundle in all_bundles:
        for entry in bundle.get('entry', []):  # Each bundle contains entries
            resource = entry.get('resource', {})
            if resource.get('resourceType') == 'Patient':
                patient_id = resource.get('id')
                name = resource.get('name', [{}])[0].get('text', 'Na')
                gender = resource.get('gender', 'Na')
                birth_date = resource.get('birthDate', 'Na')
                address = resource.get('address', [{}])[0].get('text', 'Na')
                city = resource.get('address', [{}])[0].get('city', 'Na')

                # Append the extracted data to the list
                patients_data.append({
                    'Patient ID': patient_id,
                    'Name': name,
                    'Gender': gender,
                    'Birth Date': birth_date,
                    'Address': address,
                    'City': city
                })

    return patients_data  # Return the list of dictionaries


def load_fn(ti):
    patients_data = ti.xcom_pull(task_ids='transform_task')
    patient_df = pd.DataFrame(patients_data)
    conn = psycopg2.connect(
        host="postgres",  # PostgreSQL service name from Docker Compose
        database="airflow",  # The name of the database
        user="airflow",  # The username
        password="airflow"  # The password
    )

    cursor = conn.cursor()

    create_table_query = """
        CREATE TABLE patients (
            "Patient ID" TEXT,
            "Name" TEXT,
            "Gender" TEXT,
            "Birth Date" TEXT,
            "Address" TEXT,
            "City" TEXT
        )
        """
    cursor.execute(create_table_query)
    conn.commit()
    
    for index, row in patient_df.iterrows():
            insert_query = """
            INSERT INTO patients ("Patient ID", "Name", "Gender", "Birth Date", "Address", "City")
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                row['Patient ID'],
                row['Name'],
                row['Gender'],
                row['Birth Date'],
                row['Address'],
                row['City']
            ))
    conn.commit()
    cursor.close()
    conn.close()

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract_fn,
    op_kwargs={'http_client': requests, 'initial_url': initial_url},
    execution_timeout=timedelta(minutes=30),
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform_fn,
    execution_timeout=timedelta(minutes=30),
    dag=dag
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_fn,
    execution_timeout=timedelta(minutes=30),
    dag=dag
)

# Task dependencies
extract_task >> transform_task >> load_task
