# TfL Bus Data Engineering Pipeline

## Project Overview

This project is a **Python-based ETL pipeline** designed to extract, process, and store real-time data from the **Transport for London (TfL) API**. The pipeline focuses on:

1. **Bus line statuses**  
2. **Bus stop points and their details**  
3. **Route sequences for inbound and outbound directions**  

The data collected can be used for **analytics, monitoring, or real-time dashboards** on bus service performance, stop locations, and route coverage.

---

## 🛠 Technologies Used

| Technology | Purpose |
|------------|---------|
| **Python 3.13** | Main programming language for ETL scripts |
| **Requests** | Fetch data from TfL REST API |
| **Pandas** | Data manipulation and CSV export |
| **ThreadPoolExecutor (concurrent.futures)** | Parallel API requests with rate limiting |
| **TfL Unified API** | Source of bus line, stop, and route data |
| **Astronomer CLI (Astro CLI)** | Orchestrate ETL pipelines with Airflow |
| **File System & CSV** | Storing extracted data with timestamped filenames |

---

## 📦 Project Structure



- ETL scripts extract raw datasets in parallel with **rate limiting** (50 requests/min)  
- `transform_merge.py` combines all three outputs into a **clean, analytics-ready dataset**  
- Astronomer CLI can orchestrate and schedule this entire pipeline in **Airflow**  

---

## ⚙️ Key Features

- **Threaded requests with rate limiting** — prevents exceeding TfL API limit (50 requests/min).  
- **Automatic CSV naming with timestamp** — avoids overwriting and supports versioning.  
- **Auto-detection of latest input files** — simplifies chained ETL steps.  
- **Modular utilities (`tfl_utils.py`)** — centralizes API requests, rate-limiting, and file handling.  
- **Parallelized API calls** — extracts data efficiently without hitting TfL limits.  
- **Transformation layer** — merges stop points, stop details, and route sequences into a single dataset.  
- **Airflow orchestration with Astro CLI** — schedule ETL and transformation tasks automatically.  

---

## 📊 Insights You Can Gain

- Real-time **bus service status monitoring** (`Good Service`, delays, disruptions).  
- **Bus stop geospatial distribution** with latitude and longitude for mapping.  
- **Route coverage**: which stops are served by each bus line and direction.  
- Identify **frequent disruption patterns** across bus lines.  
- Prepare datasets for **dashboards or predictive modeling** on service reliability.

---

## ⚠️ Limitations

- **Rate-limited API**: currently set to 50 requests/min, so full extraction may take several minutes.  
- **TfL API availability**: downtime or connectivity issues may interrupt data extraction.  
- **Data completeness**: Some stops or lines may have missing or outdated metadata in TfL API.  
- **No historical storage**: Each run generates snapshot CSVs; time-series storage is up to the user.  

---

## 🚀 Possible Extensions

1. **Data Warehouse Integration**  
   - Load CSVs into PostgreSQL, Redshift, or BigQuery for analytics.  

2. **Advanced Dashboarding**  
   - Visualize bus line statuses, stop locations, and route coverage using Power BI, Tableau, or Plotly Dash.  

3. **Predictive Analytics**  
   - Forecast delays and bus occupancy based on historical patterns.  
   - Identify high-risk lines for service disruptions.  

4. **Additional TfL Data Sources**  
   - Tube lines, roadworks, traffic disruption feeds.  

5. **Data Quality Checks**  
   - Implement automated validation (e.g., missing stops, invalid coordinates).  




Notes

All scripts respect TfL API limits and include retries.

Scripts are modular and can run independently or as a full Airflow DAG.

Each CSV file contains a snapshot of data at the time of extraction.

Transformation layer combines raw datasets into a clean table for analysis.
