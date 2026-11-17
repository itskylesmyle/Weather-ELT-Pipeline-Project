import json
import os
import subprocess
from datetime import timedelta

import pendulum
import requests
import snowflake.connector
from cryptography.hazmat.primitives import serialization

try:
    from airflow.sdk import dag, task  # Airflow 3
except ImportError:
    from airflow.decorators import dag, task  # Airflow 2

API_URL = 'https://api.open-meteo.com/v1/forecast'
HOURLY_VARS = 'temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m'
DBT_BIN = '/usr/local/airflow/dbt_venv/bin/dbt'
DBT_DIR = '/usr/local/airflow/include/dbt/weather_dbt'

CITIES = [
    {'name': 'Salt Lake City', 'lat': 40.7608, 'lon': -111.8910},
    {'name': 'Provo', 'lat': 40.2338, 'lon': -111.6585},
    {'name': 'Park City', 'lat': 40.6461, 'lon': -111.4980},
    {'name': 'Lehi', 'lat': 40.3916, 'lon': -111.8508},
    {'name': 'St George', 'lat': 37.0965, 'lon': -113.5684},
]


def get_snowflake_connection():
    """Connect to Snowflake using key-pair authentication."""
    with open(os.environ['SNOWFLAKE_PRIVATE_KEY_PATH'], 'rb') as key_file:
        private_key = serialization.load_pem_private_key(key_file.read(), password=None)
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        user=os.environ['SNOWFLAKE_USER'],
        private_key=private_key_bytes,
        role=os.environ['SNOWFLAKE_ROLE'],
        warehouse=os.environ['SNOWFLAKE_WAREHOUSE'],
        database=os.environ['SNOWFLAKE_DATABASE'],
    )


def run_dbt(command):
    """Run a dbt command in its own virtualenv and fail the task if dbt fails."""
    result = subprocess.run(
        [
            DBT_BIN, command,
            '--project-dir', DBT_DIR,
            '--profiles-dir', DBT_DIR,
            '--target-path', '/tmp/dbt_target',
            '--log-path', '/tmp/dbt_logs',
        ],
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f'dbt {command} failed, see the log above')


@dag(
    dag_id='weather_elt',
    start_date=pendulum.datetime(2026, 11, 4, tz='UTC'),  # about a week ago
    schedule='@daily',
    catchup=True,
    max_active_runs=1,
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=2)},
    tags=['portfolio', 'snowflake', 'dbt'],
)
def weather_elt():

    @task
    def create_raw_table():
        conn = get_snowflake_connection()
        try:
            conn.cursor().execute(
                'CREATE TABLE IF NOT EXISTS RAW.OPEN_METEO_RAW ('
                'CITY_NAME STRING, LATITUDE FLOAT, LONGITUDE FLOAT, '
                'DATA_DATE DATE, PAYLOAD VARIANT, '
                'LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP())'
            )
        finally:
            conn.close()

    @task
    def extract_and_load(city, run_date):
        # Extract: one city, one day, hourly values in UTC
        response = requests.get(
            API_URL,
            params={
                'latitude': city['lat'],
                'longitude': city['lon'],
                'hourly': HOURLY_VARS,
                'start_date': run_date,
                'end_date': run_date,
                'timezone': 'America/Denver',
            },
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()

        # Load: delete-then-insert in one transaction so reruns are safe
        conn = get_snowflake_connection()
        try:
            cur = conn.cursor()
            cur.execute('BEGIN')
            cur.execute(
                'DELETE FROM RAW.OPEN_METEO_RAW WHERE CITY_NAME = %s AND DATA_DATE = %s::DATE',
                (city['name'], run_date),
            )
            cur.execute(
                'INSERT INTO RAW.OPEN_METEO_RAW '
                '(CITY_NAME, LATITUDE, LONGITUDE, DATA_DATE, PAYLOAD) '
                'SELECT %s, %s, %s, %s::DATE, PARSE_JSON(%s)',
                (city['name'], city['lat'], city['lon'], run_date, json.dumps(payload)),
            )
            cur.execute('COMMIT')
        except Exception:
            conn.cursor().execute('ROLLBACK')
            raise
        finally:
            conn.close()

    @task
    def dbt_run():
        run_dbt('run')

    @task
    def dbt_test():
        run_dbt('test')

    created = create_raw_table()
    loaded = extract_and_load.partial(run_date='{{ ds }}').expand(city=CITIES)
    created >> loaded >> dbt_run() >> dbt_test()


weather_elt()