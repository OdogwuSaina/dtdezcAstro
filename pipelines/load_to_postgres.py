import pandas as pd
from sqlalchemy import create_engine, text
import re

def load_bus_metrics_to_postgres(df, table_name, conn_str):
    """
    Creates table if not exists and loads DataFrame into PostgreSQL.
    Ensures snake_case columns for SQL compatibility.
    """
    # Normalize column names to snake_case
    df.columns = [re.sub(r'(?<!^)(?=[A-Z])', '_', c).lower() for c in df.columns]

    # Connect to Postgres
    engine = create_engine(conn_str)

    # Create table if not exists
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        line_id TEXT,
        date DATE,
        avg_delay_duration DOUBLE PRECISION,
        delay_frequency INT,
        affected_stop_count INT,
        good_service_count INT,
        total_reports INT,
        uptime_percentage DOUBLE PRECISION
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))

    # 4️⃣ Load data
    df.to_sql(
        table_name,
        engine,
        if_exists='append',
        index=False,
        method='multi'
    )

    print(f"{len(df)} records successfully loaded into {table_name}")
