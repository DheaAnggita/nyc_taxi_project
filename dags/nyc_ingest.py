from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os, requests, duckdb

default_args = {
    'owner': 'dhea',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DATA_PATH = os.path.expanduser("~/nyc_taxi_project/data")
DB_PATH = os.path.expanduser("~/nyc_taxi_project/nyc_taxi.duckdb")

def download_data():
    os.makedirs(DATA_PATH, exist_ok=True)
    months = ["2023-01","2023-02","2023-03"]
    for m in months:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{m}.parquet"
        out = os.path.join(DATA_PATH, f"yellow_tripdata_{m}.parquet")
        if not os.path.exists(out):
            print("Downloading:", url)
            r = requests.get(url)
            with open(out, "wb") as f:
                f.write(r.content)
    print("Download done.")

def load_duckdb():
    import duckdb
    con = duckdb.connect(DB_PATH)
    con.execute("DROP TABLE IF EXISTS trips_raw")
    for m in ["2023-01","2023-02","2023-03"]:
        parquet = os.path.join(DATA_PATH, f"yellow_tripdata_{m}.parquet")
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS trips_raw AS SELECT * FROM parquet_scan('{parquet}') LIMIT 0;
        """)
        con.execute(f"""
            INSERT INTO trips_raw SELECT * FROM parquet_scan('{parquet}');
        """)
    con.close()
    print("Data loaded into DuckDB.")

with DAG("nyc_ingestion_local",
         default_args=default_args,
         start_date=datetime(2025,11,1),
         schedule_interval=None,
         catchup=False) as dag:
    
    t1 = PythonOperator(task_id="download_data", python_callable=download_data)
    t2 = PythonOperator(task_id="load_duckdb", python_callable=load_duckdb)
    t1 >> t2