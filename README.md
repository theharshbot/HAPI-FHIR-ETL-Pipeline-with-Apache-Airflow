# HAPI-FHIR-ETL-Pipeline-with-Apache-Airflow
This project implements an ETL (Extract, Transform, Load) pipeline using Apache Airflow to process patient data from the HAPI FHIR server. The pipeline extracts patient information, transforms it into a structured format, and loads it into a PostgreSQL database.


This repository contains an ETL pipeline built using Apache Airflow to extract, transform, and load patient data from the HAPI FHIR server into a PostgreSQL database. The pipeline automates the data flow to handle FHIR patient records efficiently, transforming raw JSON data into a structured format suitable for analytics and storage.

Table of Contents
Features
Architecture
Installation
Configuration
Usage
Contributing
License
Features
Automated ETL: Extracts patient data from the HAPI FHIR server, processes it, and stores it in PostgreSQL.
Airflow DAG: Uses PythonOperators within Airflow for seamless orchestration of ETL tasks.
Scalable Data Handling: Handles multiple FHIR bundles using pagination for large datasets.
PostgreSQL Integration: Creates a table in PostgreSQL and inserts transformed patient data.
Architecture
This ETL pipeline is divided into three tasks:

Extract: Retrieves patient data from the HAPI FHIR server using HTTP requests with pagination to process multiple pages of data. The FHIR patient resources are collected in JSON format.
Transform: The raw JSON data is transformed into a structured format. Patient attributes such as Patient ID, Name, Gender, Birth Date, Address, and City are extracted and normalized.
Load: The transformed data is loaded into a PostgreSQL table. If the table does not exist, it is created dynamically during the process.
DAG Structure
plaintext
Copy code
extract_task --> transform_task --> load_task
Installation
Prerequisites
Python 3.7+
Apache Airflow 2.x
PostgreSQL
Docker (Optional)
Python Dependencies
Ensure the following Python libraries are installed:

bash
Copy code
pip install pandas requests psycopg2 airflow
PostgreSQL Setup
Set up a PostgreSQL database locally or through Docker using the following command:
bash
Copy code
docker run --name postgres -e POSTGRES_USER=airflow -e POSTGRES_PASSWORD=airflow -d -p 5432:5432 postgres
Create a database called airflow within PostgreSQL.
Configuration
Airflow Configuration: Make sure Airflow is installed and running on your machine.
DAG Configuration: Place the provided HAPI_FHIR_ETL.py file into your Airflow DAGs directory.
PostgreSQL Connection: Update the PostgreSQL credentials if necessary in the load_fn function in HAPI_FHIR_ETL.py.
Usage
Clone the repository:
bash
Copy code
git clone https://github.com/your-username/hapi-fhir-etl-pipeline.git
Start the Airflow webserver and scheduler:
bash
Copy code
airflow webserver
airflow scheduler
Navigate to the Airflow UI at http://localhost:8080, locate the HAPI_FHIR_ETL DAG, and trigger it.
Monitor the ETL tasks (extract_task, transform_task, load_task) through the Airflow UI.
Contributing
Contributions are welcome! If you have suggestions for improvements or find bugs, please create an issue or submit a pull request.
