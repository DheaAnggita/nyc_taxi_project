import duckdb
import os

db_path = "nyc_taxi.duckdb"
data_path = "data"

con = duckdb.connect(db_path)
con.execute("DROP TABLE IF EXISTS trips_raw")

files = [
    "yellow_tripdata_2023-01.parquet",
    "yellow_tripdata_2023-02.parquet",
    "yellow_tripdata_2023-03.parquet",
]

for f in files:
    parquet_path = os.path.join(data_path, f)
    if not os.path.exists(parquet_path):
        print(f"File tidak ditemukan: {parquet_path}")
        continue
    print(f"📦 Memuat data: {f}")
    con.execute(f"CREATE TABLE IF NOT EXISTS trips_raw AS SELECT * FROM parquet_scan('{parquet_path}') LIMIT 0")
    con.execute(f"INSERT INTO trips_raw SELECT * FROM parquet_scan('{parquet_path}')")

count = con.execute("SELECT COUNT(*) FROM trips_raw").fetchone()[0]
print("Selesai!")
print(f"File database: {db_path}")
print(f"Jumlah baris di tabel trips_raw: {count:,}")

con.close()
