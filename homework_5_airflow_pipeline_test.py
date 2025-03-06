from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import timedelta, datetime
import requests
import pandas as pd

# Establish Snowflake Connection
def get_snowflake_cursor():
    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_connection')
    snowflake_connection = snowflake_hook.get_conn()
    return snowflake_connection.cursor()

# Task 1: Extract Data from Alpha Vantage API
@task
def fetch_stock_data(stock_symbol="AAPL"):
    api_key = Variable.get("alpha_vantage_api_key")
    api_url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&apikey={api_key}"
    response = requests.get(api_url)
    api_data = response.json()

    # Extract time series data
    stock_time_series = api_data.get('Time Series (Daily)', {})
    stock_df = pd.DataFrame.from_dict(stock_time_series, orient='index')
    stock_df.columns = ['open', 'high', 'low', 'close', 'volume']
    stock_df.index = pd.to_datetime(stock_df.index)

    stock_df.reset_index(inplace=True)
    stock_df.rename(columns={'index': 'trade_date'}, inplace=True)
    stock_df['symbol'] = stock_symbol

    stock_df['trade_date'] = stock_df['trade_date'].dt.strftime('%Y-%m-%d')

    stock_df = stock_df.sort_values(by="trade_date", ascending=False).head(90)

    print("Extracted Data (First 5 rows):")
    print(stock_df.head())

    return stock_df.to_dict(orient='records')

# Task 2: Transform Data
@task
def prepare_data_for_loading(stock_data):
    transformed_data = [
        (
            row['symbol'],
            row['trade_date'],
            float(row.get('open', 0)),
            float(row.get('high', 0)),
            float(row.get('low', 0)),
            float(row.get('close', 0)),
            int(row.get('volume', 0))
        )
        for row in stock_data
    ]

    print("Transformed Data (First 5 rows):")
    print(transformed_data[:5])

    return transformed_data

# Task 3: Load Data into Snowflake
@task
def insert_data_into_snowflake(data_records, destination_table):
    cursor = get_snowflake_cursor()

    try:
        cursor.execute("BEGIN;")
        cursor.execute(f"""CREATE TABLE IF NOT EXISTS {destination_table} (
            symbol STRING,
            trade_date TIMESTAMP,
            open NUMBER(38, 4),
            high NUMBER(38, 4),
            low NUMBER(38, 4),
            close NUMBER(38, 4),
            volume NUMBER(38, 0),
            PRIMARY KEY (symbol, trade_date)
        );""")

        if not data_records:
            raise ValueError("No data available for insertion.")

        cursor.execute(f"DELETE FROM {destination_table} WHERE symbol = 'AAPL'")

        insert_sql = f'''
            INSERT INTO {destination_table} (symbol, trade_date, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        '''
        cursor.executemany(insert_sql, data_records)

        cursor.execute("COMMIT;")
        print(f"✅ Successfully inserted {len(data_records)} rows into {destination_table}")

    except Exception as e:
        cursor.execute("ROLLBACK;")
        print(f"❌ Error inserting data into Snowflake: {e}")
        raise e

# DAG Configuration
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Define the DAG
with DAG(
    dag_id='stock_data_pipeline',
    default_args=default_args,
    start_date=datetime(2024, 9, 25),
    catchup=False,
    tags=['ETL'],
    schedule_interval='@daily'
) as dag:
    target_table = "STOCK_DATA"

    extracted_data = fetch_stock_data()
    transformed_data = prepare_data_for_loading(extracted_data)
    load_task = insert_data_into_snowflake(transformed_data, target_table)

    extracted_data >> transformed_data >> load_task
