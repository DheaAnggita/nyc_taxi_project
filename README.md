# NYC Taxi Data Analytics Pipeline (Airflow, dbt, Streamlit)

Proyek ini membangun pipeline analitik data end-to-end menggunakan data NYC Yellow Taxi Trip (Januari—Maret 2023) untuk mendemonstrasikan kemampuan orkestrasi, transformasi, dan visualisasi data.

## Komponen Utama

1.  **Orkestrasi (Part 1):** Apache Airflow (dengan Venv)
2.  **Transformasi (Part 2):** dbt (Data Build Tool) + DuckDB
3.  **Visualisasi (Part 3):** Streamlit + Plotly

---

## Persyaratan Sistem

* Python 3.10+
* PostgreSQL (Opsional, jika tidak, Airflow akan menggunakan SQLite default)
* **Wajib:** Akses internet stabil untuk mengunduh data Parquet (sekitar 150MB).

---

## Langkah-Langkah Setup (macOS/Linux dengan Venv)

### A. Kloning Repositori & Persiapan Lingkungan

1.  **Kloning:** Kloning repository ini ke mesin lokal Anda.
    ```bash
    git clone https://github.com/DheaAnggita/nyc_taxi_project.git
    cd nyc_taxi_project
    ```

2.  **Virtual Environment (Venv):** Buat dan aktifkan Venv.
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

3.  **Instalasi Dependensi:** Instal semua library (Airflow, dbt, Streamlit, DuckDB).
    ```bash
    pip install -r requirements.txt
    ```

### PENTING: Penyesuaian Absolute Path (WAJIB DILAKUKAN)

File `dags/nyc_taxi_transform.py` menggunakan *hardcoded absolute path* ke folder `dbt_project`. Anda **harus mengganti** path ini agar sesuai dengan lingkungan lokal Anda.

1.  **Tentukan Path Absolut Anda:**
    Navigasi ke folder `nyc_taxi_project/` Anda di terminal, lalu jalankan:
    ```bash
    echo "$(pwd)/dbt_project"
    ```

2.  **Edit File DAG:** Buka file **`dags/nyc_taxi_transform.py`** dan ganti nilai variabel `DBT_PROJECT_PATH` (baris 5 atau 6) dengan path absolut yang baru Anda dapatkan.

### B. Setup Airflow & Database

1.  **Set AIRFLOW_HOME:** Tentukan direktori kerja Airflow.
    ```bash
    export AIRFLOW_HOME=~/airflow_home 
    ```

2.  **Inisialisasi Airflow:** Buat database metadata Airflow (SQLite default) dan user admin.
    ```bash
    airflow db init
    airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
    ```

3.  **Buat Symlink DAGs:** Buat *shortcut* agar Airflow Scheduler dapat menemukan DAGs Anda yang berada di folder `dags/` proyek.
    ```bash
    # Pindahkan atau buat folder dags di AIRFLOW_HOME
    mkdir -p $AIRFLOW_HOME/dags
    # Hapus folder dags lama jika hanya berisi file README Airflow
    rm -rf $AIRFLOW_HOME/dags 
    # Buat symlink dari folder dags proyek ke AIRFLOW_HOME
    ln -s $(pwd)/dags $AIRFLOW_HOME/dags 
    ```

### C. Download Data Mentah & Setup DuckDB

1.  **Download File Parquet:** Jalankan *script* ingestion untuk mengunduh 3 file Parquet (Jan-Mar 2023) dan memuatnya ke DuckDB (`nyc_taxi.duckdb`).
    ```bash
    # PASTIKAN VENV AKTIF
    python create_duckdb.py
    ```

---

## 3. Eksekusi Pipeline End-to-End

### A. Jalankan Airflow (Orkestrasi)

Buka dua terminal baru, aktifkan `venv` di keduanya, dan jalankan Scheduler & Webserver.

| Terminal 1 (Scheduler) | Terminal 2 (Webserver) |
| :--- | :--- |
| `source venv/bin/activate` | `source venv/bin/activate` |
| `airflow scheduler` | `airflow webserver -p 8080` |

### B. Trigger Transformation (dbt Run)

1.  Akses Airflow UI: `http://localhost:8080`.
2.  Cari dan aktifkan DAG **`nyc_taxi_transform`**.
3.  **Trigger DAG** tersebut. Pipeline akan menjalankan `dbt seed`, `dbt run` (membuat Marts), dan `dbt test` (verifikasi kualitas data).

### C. Akses Dashboard Streamlit (Visualisasi)

Setelah DAG transformasi sukses, *Data Marts* Anda siap.

1.  **Hentikan** Airflow Scheduler/Webserver.
2.  **Jalankan Streamlit:**
    ```bash
    source venv/bin/activate
    streamlit run visualization/streamlit_app.py
    ```
3.  Buka URL Lokal (`http://localhost:8501`) untuk melihat *dashboard* interaktif Anda.