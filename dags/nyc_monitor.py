from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, duckdb
from airflow.utils.email import send_email

DB_PATH = os.path.expanduser("~/nyc_taxi_project/nyc_taxi.duckdb")

def check_data_freshness():
    """
    Cek kesegaran data (Data Freshness) dari tabel agg_daily_stats.
    """
    con = duckdb.connect(DB_PATH)
    try:
        max_date_tuple = con.execute("SELECT MAX(day) FROM agg_daily_stats").fetchone()
        
        if max_date_tuple is None or max_date_tuple[0] is None:
            raise ValueError("Tabel agg_daily_stats kosong atau belum diisi oleh dbt.")
            
        max_date = max_date_tuple[0]
        
        print(f"Data FRESH (Verifikasi Marts): Data terakhir yang ditemukan di Marts adalah {max_date}.")
            
    finally:
        con.close()
        
def check_row_count_anomaly():
    """
    Validasi row counts/metrik dan membandingkan current vs. historical patterns (simulasi).
    """
    con = duckdb.connect(DB_PATH)
    
    max_date_str = con.execute("SELECT MAX(day) FROM agg_daily_stats").fetchone()
    
    if max_date_str is None or max_date_str[0] is None:
        print("Data Marts belum tersedia untuk pemeriksaan anomali.")
        con.close()
        return
        
    today_date_dt = max_date_str[0]
    yesterday_date_dt = today_date_dt - timedelta(days=1)
    
    today_date = today_date_dt.strftime('%Y-%m-%d')
    yesterday_date = yesterday_date_dt.strftime('%Y-%m-%d')
    today_count_tuple = con.execute(f"SELECT trips FROM agg_daily_stats WHERE day = '{today_date}'").fetchone()
    yesterday_count_tuple = con.execute(f"SELECT trips FROM agg_daily_stats WHERE day = '{yesterday_date}'").fetchone()
    con.close()

    if today_count_tuple is None or yesterday_count_tuple is None:
        print("Data tidak lengkap untuk perbandingan anomali hari terakhir vs hari sebelumnya.")
        return

    today_trips = today_count_tuple[0]
    yesterday_trips = yesterday_count_tuple[0]
    
    if yesterday_trips > 0 and abs(today_trips - yesterday_trips) / yesterday_trips > 0.20:
        message = f"ANOMALI ROW COUNT: Trips pada {today_date} ({today_trips:,}) berbeda >20% dari {yesterday_date} ({yesterday_trips:,})."
        print(message)
    else:
        print(f"Row Count OK: Trips {today_date} ({today_trips:,}) vs {yesterday_date} ({yesterday_trips:,}).")

with DAG("nyc_taxi_monitor",
         default_args={'owner': 'dhea', 'retries': 0},
         start_date=datetime(2025, 11, 1),
         schedule_interval='*/6 * * * *', 
         catchup=False) as dag:
    
    t1_freshness = PythonOperator(task_id="check_data_freshness", python_callable=check_data_freshness)
    t2_anomaly = PythonOperator(task_id="check_row_count_anomaly", python_callable=check_row_count_anomaly)
    
    t1_freshness >> t2_anomaly