import requests
import psycopg2
from datetime import datetime

# Replace with your actual API key and database connection details
API_KEY = 'your_openweathermap_api_key'
DATABASE_CONFIG = {
  'dbname': 'airflow',
  'user': 'postgres',
  'password': 'postgres',
  'host': 'localhost',
  'port': '5432',
}


def fetch_weather_data():
  url = f"https://api.openweathermap.org/data/2.5/weather?q=London,uk&APPID=52f32eba9a829e878bdef33fe9f8e49a"
  response = requests.get(url)
  data = response.json()
  return data


def transform_data(raw_data):
  if raw_data:
    cleaned_data = {
      'city': raw_data['name'],
      'temperature': raw_data['main']['temp'] - 273.15,  # Kelvin to Celsius
      'weather': raw_data['weather'][0]['description'],
      'humidity': raw_data['main']['humidity'],
      'timestamp': datetime.utcfromtimestamp(raw_data['dt']).strftime('%Y-%m-%d %H:%M:%S')
    }
    return cleaned_data
  return None


def load_data_to_db(cleaned_data):
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
      cleaned_data['city'], cleaned_data['temperature'],
      cleaned_data['weather'], cleaned_data['humidity'],
      cleaned_data['timestamp']
    ))

    # Commit and close
    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
  raw_data = fetch_weather_data()
  print(f'Raw Data: {raw_data}')

  cleaned_data = transform_data(raw_data)
  print(f'Cleaned Data: {cleaned_data}')

  load_data_to_db(cleaned_data)
  print('Data loaded to database successfully!')
