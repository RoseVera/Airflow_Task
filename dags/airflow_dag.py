from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import psycopg2
import ta


conn_str = "postgresql://neondb_owner:npg_uBkh28WeMyXG@ep-plain-sunset-a4qz1i3u-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"


def fetch_data(**context):
    url = 'https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=5m&limit=100'
    response = requests.get(url)
    data = response.json()

    df = pd.DataFrame(data, columns=[
        'open_time', 'open', 'high', 'low', 'close', 'volume',
        'close_time'
    ])

    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(float)

    context['ti'].xcom_push(key='raw_data', value=df.to_json())

def process_data(**context):
    raw_json = context['ti'].xcom_pull(key='raw_data', task_ids='fetch_data_task')
    df = pd.read_json(raw_json)

    df['sma'] = ta.trend.sma_indicator(df['close'], window=14)
    df['ema'] = ta.trend.ema_indicator(df['close'], window=14)
    df['rsi'] = ta.momentum.rsi(df['close'], window=14)

    df = df[['open_time', 'open', 'high', 'low', 'close', 'volume', 'sma', 'ema', 'rsi']]
    df.dropna(inplace=True)

    context['ti'].xcom_push(key='processed_data', value=df.to_json())

def insert_to_postgres(**context):
    processed_json = context['ti'].xcom_pull(key='processed_data', task_ids='process_data_task')
    df = pd.read_json(processed_json)

    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO btc_usdt_technical (
                open_time, open, high, low, close, volume, sma, ema, rsi
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (open_time) DO NOTHING;
        """, tuple(row))

    conn.commit()
    cur.close()
    conn.close()



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='dag_pipeline',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
    tags=['crypto', 'btc', 'technical'],
    description='Fetch BTCUSDT data, process and insert into Neon DB',
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_task',
        python_callable=fetch_data,
        provide_context=True,
    )

    process_task = PythonOperator(
        task_id='process_task',
        python_callable=process_data,
        provide_context=True,
    )

    insert_task = PythonOperator(
        task_id='insert_postgres_task',
        python_callable=insert_to_postgres,
        provide_context=True,
    )

    fetch_task >> process_task >> insert_task
