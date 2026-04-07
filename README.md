# Distributed Log Intelligence & Threat Detection

A Big Data pipeline built with **Apache Spark (PySpark)** designed to ingest, structure, and analyze large-scale server logs in real-time to detect security vulnerabilities.

## System Architecture
The pipeline utilizes **Spark Structured Streaming** and **Spark SQL** for "Schema-on-Read" processing, transforming unstructured continuous Apache web logs into queryable threat intelligence.

* **Data Ingestion:** Simulated 100k+ record log stream representing a live web server.
* **Processing Engine:** Distributed Spark Cluster running in Local Mode.
* **Storage Format:** Real-time in-memory sinking with final analytical outputs rendered automatically.

## Security Logic
The real-time analyzer implements heuristic patterns to detect malicious behaviors:
1. **SQL Injection:** Identifying `DROP` or `SELECT` keywords in URL payload parameters.
2. **Brute Force/Unauthorized Access:** Monitoring `401 Unauthorized` status codes at scale.
3. **Reconnaissance:** Detecting `Nmap` and other network scanners via User-Agent analysis.

## Prerequisites
- **Python 3.x**
- **Java (JDK 8, 11, or 17)** is strictly required for the Apache Spark JVM to run. You can verify it by running `java -version`.

## Setup & Installation

1. **Install Python Dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Windows Specific Setup (PySpark Binaries):**
   If you are running this on Windows, PySpark requires specific Hadoop binaries (`winutils.exe`, `hadoop.dll`) to interact with the local filesystem. We have provided an automated setup script to fetch these for you:
   ```bash
   python setup_winutils.py
   ```
   *This automatically creates a `hadoop/` folder and points PySpark's internal paths to it.*

## Usage: Running the Pipeline

This project simulates a 24/7 web server and an active analytics listener. You will need to run two scripts simultaneously in **two separate terminals**.

**Terminal 1: Start the Log Generator**
This script continuously creates fake traffic logs inside the `input_logs/` folder.
```bash
python log_generator.py
```

**Terminal 2: Start the PySpark Analyser**
This script uses a PySpark streaming session to monitor the incoming logs, parse them via Regex, classify threats, map synthetic IPs, and generate visual charts.
```bash
python distributed_log_analyser.py
```

*Both scripts will run continuously until you manually stop them by pressing `Ctrl + C` in each terminal.*

## Outputs & Analytics
As the log analyser runs, an `output/` directory will be created in the project root. PySpark will automatically query the streaming engine every 15 seconds and update the following visualizations:
- `threat_summary.png` (Bar chart quantifying the types of attacks)
- `threat_locations.png` (Pie chart detailing the synthetic origin mapping of the traffic)

## Key Achievements
- Processed 100,000+ entries using **vectorized Spark functions**.
- Utilized native PySpark SQL data type castings to aggressively bypass Python cloudpickle serialization bottlenecks (`RecursionError`).
- Implemented robust real-time metrics generation mapping raw text streams into analytical insights.
- Developed a modular streaming architecture compatible with **AWS EMR** or **Databricks**.
