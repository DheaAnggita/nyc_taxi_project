import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from datetime import datetime
import numpy as np

st.set_page_config(layout="wide", page_title="NYC Taxi Data Analytics Dashboard")

DB_PATH = "nyc_taxi.duckdb"

@st.cache_resource
def get_db_connection():
    """Membuat dan meng-cache koneksi DuckDB."""
    try:
        return duckdb.connect(database=DB_PATH, read_only=True)
    except Exception as e:
        st.error(f"Gagal terhubung ke database DuckDB: {e}. Pastikan file {DB_PATH} ada dan pipeline dbt sudah sukses.")
        return None

con = get_db_connection()

if con is None:
    st.stop()
    

@st.cache_data
def load_base_data(query):
    """Mengambil data dari DuckDB menggunakan caching Streamlit."""
    return con.execute(query).fetchdf()

df_zones = load_base_data("SELECT location_id, borough, zone FROM dim_zones")
df_agg = load_base_data("SELECT * FROM agg_daily_stats ORDER BY day")



st.title("NYC Taxi Trip Data Insights")
st.markdown("Dashboard Analitik berdasarkan Data Marts (dbt) dari Januari-Maret 2023.")

st.sidebar.header("Filter Data")

min_date_project = datetime(2023, 1, 1).date()
max_date = df_agg['day'].max().date()
default_start = min_date_project 
default_end = max_date 


date_range = st.sidebar.date_input(
    "Pilih Rentang Tanggal",
    (default_start, default_end),
    min_value=min_date_project,
    max_value=max_date
)

selected_boroughs = st.sidebar.multiselect(
    "Pilih Borough (Pickup)",
    options=df_zones['borough'].unique().tolist(),
    default=df_zones['borough'].unique().tolist()
)

payment_options = {1: 'Credit Card', 2: 'Cash', 3: 'No Charge', 4: 'Dispute', 5: 'Unknown', 6: 'Voided Trip'}
payment_filter_keys = st.sidebar.multiselect(
    "Pilih Metode Pembayaran",
    options=list(payment_options.values()),
    default=list(payment_options.values())
)
payment_filter_ids = [k for k, v in payment_options.items() if v in payment_filter_keys]


date_filter = f"f.pickup_date BETWEEN DATE '{date_range[0]}' AND DATE '{date_range[1]}'"

if not selected_boroughs:
    borough_filter = "1=1"
else:
    borough_filter = f"d_pu.borough IN ({str(selected_boroughs)[1:-1]})"

if not payment_filter_ids:
    payment_filter = "1=1"
else:
    payment_filter = f"f.payment_type IN ({','.join(map(str, payment_filter_ids))})"


st.header("1. KPI Summary & 2. Trend Analysis")

kpi_query = f"""
SELECT 
    SUM(f.passenger_count) as total_passengers,
    SUM(f.trip_distance) as total_distance_miles,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_fare,
    COUNT(f.surrogate_key) as total_trips
FROM fct_trips f
JOIN dim_zones d_pu ON f.pu_location_id = d_pu.location_id
WHERE {date_filter} AND {borough_filter} AND {payment_filter}
"""
df_kpi = load_base_data(kpi_query)
df_kpi_metrics = df_kpi.iloc[0]

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Trips", f"{df_kpi_metrics['total_trips']:,.0f}")
col2.metric("Total Revenue", f"${df_kpi_metrics['total_revenue']:,.2f}")
col3.metric("Avg Fare per Trip", f"${df_kpi_metrics['avg_fare']:.2f}")
col4.metric("Total Passengers", f"{df_kpi_metrics['total_passengers']:,.0f}")


df_trend_filtered = df_agg[
    (df_agg['day'] >= pd.to_datetime(date_range[0])) & 
    (df_agg['day'] <= pd.to_datetime(date_range[1]))
]
st.subheader("Tren Harian Trips dan Revenue")
fig_trend = px.line(
    df_trend_filtered, 
    x='day', 
    y=['trips', 'revenue'], 
    labels={'trips': 'Trips', 'revenue': 'Revenue', 'day': 'Tanggal'}, 
    title='Tren Harian Trips vs. Revenue'
)
st.plotly_chart(fig_trend, use_container_width=True)


st.header("3. Top Zones, Demand Heatmap, & Payment Breakdown")

col_left, col_right = st.columns(2)

with col_left:
    QUERY_TOP_ZONES = f"""
    SELECT 
        d_pu.borough || ' - ' || d_pu.zone AS pickup_zone, 
        SUM(f.total_amount) AS total_revenue 
    FROM fct_trips f
    JOIN dim_zones d_pu ON f.pu_location_id = d_pu.location_id 
    WHERE {date_filter} AND {borough_filter} AND {payment_filter}
    GROUP BY 1
    HAVING SUM(f.total_amount) > 0 
    ORDER BY 2 DESC
    LIMIT 10
    """
    df_top_zones = load_base_data(QUERY_TOP_ZONES)
    st.subheader("Top 10 Zones (Pickup) Berdasarkan Revenue")
    fig_top_zones = px.bar(
        df_top_zones, 
        x='total_revenue', 
        y='pickup_zone', 
        orientation='h', 
        labels={'total_revenue': 'Revenue ($)', 'pickup_zone': 'Zone'},
        color='total_revenue',
        color_continuous_scale=px.colors.sequential.Teal
    )
    st.plotly_chart(fig_top_zones, use_container_width=True)

    QUERY_PAYMENT = f"""
    SELECT 
        CASE f.payment_type 
            WHEN 1 THEN 'Credit Card' 
            WHEN 2 THEN 'Cash' 
            WHEN 3 THEN 'No Charge'
            WHEN 4 THEN 'Dispute'
            ELSE 'Other' 
        END AS payment_method,
        COUNT(*) AS total_trips
    FROM fct_trips f
    JOIN dim_zones d_pu ON f.pu_location_id = d_pu.location_id 
    WHERE {date_filter} AND {borough_filter} AND {payment_filter}
    GROUP BY 1
    """
    df_payment = load_base_data(QUERY_PAYMENT)
    st.subheader("Proporsi Metode Pembayaran")
    fig_payment = px.pie(
        df_payment, 
        values='total_trips', 
        names='payment_method', 
        hole=.3,
        color_discrete_sequence=px.colors.sequential.RdBu
    )
    st.plotly_chart(fig_payment, use_container_width=True)


with col_right:
    QUERY_HEATMAP = f"""
    SELECT 
        EXTRACT(HOUR FROM f.tpep_pickup_datetime) AS pickup_hour,
        d_pu.borough AS pickup_borough, -- FIX: Mengganti hari menjadi Borough
        COUNT(*) AS total_trips
    FROM fct_trips f
    JOIN dim_zones d_pu ON f.pu_location_id = d_pu.location_id 
    WHERE {date_filter} AND {borough_filter} AND {payment_filter}
    GROUP BY 1, 2
    """
    df_heatmap_raw = load_base_data(QUERY_HEATMAP)
    
    df_heatmap_pivot = df_heatmap_raw.pivot_table(
        index='pickup_hour', 
        columns='pickup_borough', 
        values='total_trips'
    ).fillna(0)
    
    st.subheader("Demand Heatmap (Trips per Jam & Wilayah)")
    fig_heatmap = px.imshow(
        df_heatmap_pivot.values,
        x=df_heatmap_pivot.columns.tolist(), 
        y=df_heatmap_pivot.index.tolist(),
        color_continuous_scale="Viridis",
        labels=dict(x="Wilayah Pickup (Borough)", y="Jam (24h)", color="Total Trips")
    )
    st.plotly_chart(fig_heatmap, use_container_width=True)