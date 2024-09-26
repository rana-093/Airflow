from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import json
import psycopg2
from datetime import datetime

DATABASE_CONFIG = {
  'dbname': 'airflow',
  'user': 'postgres',
  'password': 'postgres',
  'host': 'localhost',
  'port': '5432',
}

default_args = {
  'owner': 'airflow',
  'start_date': days_ago(1),
  'retries': 1
}

dag = DAG(
  'weather_data_etl',
  default_args=default_args,
  description='ETL pipeline to fetch weather data and store in PostgreSQL',
  schedule_interval='* * * * *',
)


def fetch_weather_data(**kwargs):
    url = f"https://api.openweathermap.org/data/2.5/weather?q=London,uk&APPID=52f32eba9a829e878bdef33fe9f8e49a"
    response = requests.get(url)
    data = response.json()
    ti = kwargs['ti']
    ti.xcom_push(key='weather_data', value=data)


def transform_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids='fetch_weather_data', key='weather_data')
    print(f'raw_data is: {raw_data}')
    if raw_data:
        cleaned_data = {
          'city': raw_data['name'],
          'temperature': raw_data['main']['temp'] - 273.15,  # Kelvin to Celsius
          'weather': raw_data['weather'][0]['description'],
          'humidity': raw_data['main']['humidity'],
          'timestamp': datetime.utcfromtimestamp(raw_data['dt']).strftime('%Y-%m-%d %H:%M:%S')
        }
        ti.xcom_push(key='cleaned_data', value=cleaned_data)
    return None


def load_data_to_db(**kwargs):
    ti = kwargs['ti']
    cleaned_data = ti.xcom_pull(task_ids='transform_data', key='cleaned_data')
    print(f'Inside load_data_to_db: {cleaned_data}')

    if cleaned_data:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        cur = conn.cursor()

        # Create a table if it doesn't exist
        create_table_query = """
            CREATE TABLE IF NOT EXISTS weather_data (
                city VARCHAR(50),
                temperature FLOAT,
                weather VARCHAR(100),
                humidity INT,
                timestamp TIMESTAMP
            );
            """
        cur.execute(create_table_query)

        # Insert data into the table
        insert_query = """
            INSERT INTO weather_data (city, temperature, weather, humidity, timestamp)
            VALUES (%s, %s, %s, %s, %s);
            """
        cur.execute(insert_query, (
          cleaned_data['city'], cleaned_data['temperature'], cleaned_data['weather'], cleaned_data['humidity'],
          cleaned_data['timestamp']))

        # Commit and close
        conn.commit()
        cur.close()
        conn.close()

fetch_weather_data_task = PythonOperator(
  task_id='fetch_weather_data',
  python_callable=fetch_weather_data,
  provide_context=True,
  dag=dag,
)

transform_data_task = PythonOperator(
  task_id='transform_data',
  python_callable=transform_data,
  provide_context=True,
  dag=dag,
)

load_data_to_db_task = PythonOperator(
  task_id='load_data_to_db',
  python_callable=load_data_to_db,
  provide_context=True,
  dag=dag,
)

fetch_weather_data_task >> transform_data_task >> load_data_to_db_task
