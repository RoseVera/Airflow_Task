from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd 
import psycopg2
from typing import List
import io  
 
conn_str = "postgresql://neondb_owner:npg_kzVOe8G5IEuo@ep-dark-frost-a4qkadpq-pooler.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
def sma(values: List[float], window: int):
    if len(values) < window:
        return None
    return sum(values[-window:]) / window

def ema(values: List[float], window: int, prev_ema=None):
    if len(values) < window:
        return None
    k = 2 / (window + 1)
    if prev_ema is None:
        return sma(values, window)
    return values[-1] * k + prev_ema * (1 - k)

def rsi(values: List[float], window: int = 14):
    if len(values) < window + 1:
        return None
    gains = 0
    losses = 0
    for i in range(-window, 0):
        diff = values[i] - values[i - 1]
        if diff > 0:
            gains += diff
        else:
            losses -= diff
    if losses == 0:
        return 100
    rs = gains / losses
    return 100 - (100 / (1 + rs))
    
def fetch_data(**context):
    print("📥 fetch_data çalışıyor...")

    def get_crypto_data(symbol="BTCUSDT", interval="5m", start=None, end=None):
        url = "https://api.binance.com/api/v3/klines"
        limit = 1000
        all_data = []

        if start is None or end is None:
            raise ValueError("Start and end datetime must be provided")

        start_ts = int(start.timestamp() * 1000)
        end_ts = int(end.timestamp() * 1000)

        while True:
            params = {
                "symbol": symbol,
                "interval": interval,
                "limit": limit,
                "startTime": start_ts,
                "endTime": end_ts,
            }
            response = requests.get(url, params=params)
            data = response.json()

            if not data or (isinstance(data, dict) and data.get("code")):
                print(f"❌ API error: {data.get('msg', 'Unknown error')}")
                return pd.DataFrame()  # boş döndür

            all_data.extend(data)

            if len(data) < limit:
                break

            last_open_time = data[-1][0]
            start_ts = last_open_time + 1

            if start_ts > end_ts:
                break

        df = pd.DataFrame(all_data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
        ])

        df["open_time"] = pd.to_datetime(df["open_time"], unit='ms')
        df["close_time"] = pd.to_datetime(df["close_time"], unit='ms')

        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)

        return df[["open_time", "open", "high", "low", "close", "volume"]]

    # 👉 Şu anki zamanı al ve 500 dakika geriden veri çek
    now = datetime.utcnow()
    start_time = now - timedelta(minutes=500)
    end_time = now

    df = get_crypto_data(symbol="BTCUSDT", interval="5m", start=start_time, end=end_time)

    if df.empty:
        print("⚠️ Veri alınamadı veya API reddetti.")
    else:
        print(f"✅ {len(df)} satır veri alındı.")
        print(df.head())

    context['ti'].xcom_push(key='raw_data', value=df.to_json())



def process_data(**context):
    print("process taski yapiyor su an")

    raw_json = context['ti'].xcom_pull(key='raw_data', task_ids='fetch_task')
    df = pd.read_json(raw_json)



    window = 14
    closes = df['close'].tolist()

    sma_list = []
    ema_list = []
    rsi_list = []

    prev_ema_value = None

    for i in range(len(df)):
        close_slice = closes[:i+1]

        sma_val = sma(close_slice, window)
        sma_list.append(sma_val)

        ema_val = ema(close_slice, window, prev_ema_value)
        ema_list.append(ema_val)
        prev_ema_value = ema_val

        rsi_val = rsi(close_slice, window)
        rsi_list.append(rsi_val)

    df['sma'] = sma_list
    df['ema'] = ema_list
    df['rsi'] = rsi_list

    df = df[['open_time', 'open', 'high', 'low', 'close', 'volume', 'sma', 'ema', 'rsi']]
    df.dropna(inplace=True)

    context['ti'].xcom_push(key='processed_data', value=df.to_json())


def insert_to_postgres(**context):
    print("insert taski yapiyor su an")
    processed_data = context['ti'].xcom_pull(key='processed_data', task_ids='process_task')
    df = pd.read_json(processed_data)
 
    print("📊 DataFrame shape:", df.shape)
    print("📌 Sample rows:")
    print(df.head()) 

    if df.empty:
        print("⚠️ DataFrame is EMPTY. No data to insert!")
        return
     

    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()

  
    try:
        conn = psycopg2.connect(conn_str)
        print("✅ Connected to DB")
    except Exception as e:
        print("❌ Connection failed:", e)


    for _, row in df.iterrows():

        cur.execute("""
            INSERT INTO btc_usdt_technical (open_time, open, high, low, close, volume, sma, ema, rsi) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (open_time) DO NOTHING;
        """, tuple(row))

    conn.commit()
    cur.close()
    conn.close()



default_args = {
    'start_date': datetime(2025, 3, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='dag_pipeline',
    default_args=default_args,
    schedule='*/5 * * * *', 
    catchup=False
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_task',
        python_callable=fetch_data,

    )

    process_task = PythonOperator(
        task_id='process_task',
        python_callable=process_data,
       
    )

    insert_task = PythonOperator(
        task_id='insert_postgres_task',
        python_callable=insert_to_postgres,
     
    )

    fetch_task >> process_task >> insert_task
