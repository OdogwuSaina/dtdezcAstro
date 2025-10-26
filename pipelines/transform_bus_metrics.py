import os
import pandas as pd
import numpy as np

def transform_bus_metrics(bus_status_path, stops_path, stop_points_path):
    """Transform TfL bus data: merge sources and compute metrics."""

    # 1️⃣ Load CSVs
    bus_status = pd.read_csv(bus_status_path)
    stops = pd.read_csv(stops_path)
    stop_points = pd.read_csv(stop_points_path)

    # 2️⃣ Merge bus_status with stop_points (by lineId)
    merged_1 = pd.merge(bus_status, stop_points, on='lineId', how='left')

    # 3️⃣ Merge with stops (by stopPointId -> ATCOCode)
    merged_final = pd.merge(
        merged_1,
        stops,
        left_on='stopPointId',
        right_on='ATCOCode',
        how='left'
    )

    # 4️⃣ Select relevant columns
    merged_final = merged_final[[
        'lineId', 'lineName', 'statusSeverity', 'statusSeverityDescription',
        'reason', 'fromDate', 'toDate', 'created', 'modified',
        'stopPointId', 'CommonName', 'Street', 'Town', 'Longitude', 'Latitude'
    ]]

    # 5️⃣ Clean and compute types
    df = merged_final.copy()
    df['fromDate'] = pd.to_datetime(df['fromDate'], errors='coerce')
    df['toDate'] = pd.to_datetime(df['toDate'], errors='coerce')
    df['created'] = pd.to_datetime(df['created'], errors='coerce')
    df['statusSeverity'] = pd.to_numeric(df['statusSeverity'], errors='coerce')
    df['date'] = df['created'].dt.date
    df['delay_flag'] = df['statusSeverity'] < 10
    df['delay_duration_min'] = (df['toDate'] - df['fromDate']).dt.total_seconds() / 60

    # 6️⃣ Metrics
    avg_delay = (
        df[df['delay_flag']]
        .groupby(['lineId', 'date'])
        .agg(avg_delay_duration=('delay_duration_min', 'mean'))
        .reset_index()
    )

    delay_freq = (
        df[df['delay_flag']]
        .groupby(['lineId', 'date'])
        .size()
        .reset_index(name='delay_frequency')
    )

    uptime = (
        df.groupby('lineId')
        .agg(
            good_service_count=('statusSeverity', lambda x: (x == 10).sum()),
            total_reports=('statusSeverity', 'count')
        )
        .assign(uptime_percentage=lambda x: (x['good_service_count'] / x['total_reports']) * 100)
        .reset_index()
    )

    affected_stops = (
        df[df['delay_flag']]
        .groupby(['lineId', 'date'])
        .agg(affected_stop_count=('stopPointId', pd.Series.nunique))
        .reset_index()
    )

    # 7️⃣ Merge all metrics
    metrics = (
        avg_delay
        .merge(delay_freq, on=['lineId', 'date'], how='outer')
        .merge(affected_stops, on=['lineId', 'date'], how='outer')
        .merge(uptime, on='lineId', how='left')
        .fillna(0)
    )

    print(f"✅ Computed metrics for {metrics['lineId'].nunique()} bus lines.")
    return metrics
