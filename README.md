# Distributed Log Intelligence & Threat Detection

A Big Data pipeline built with **Apache Spark (PySpark)** designed to ingest, structure, and analyze large-scale server logs for security vulnerabilities.

## System Architecture
The pipeline utilizes **Spark SQL** for "Schema-on-Read" processing, transforming unstructured Apache logs into a queryable data lake format.

* **Data Ingestion:** Simulated 100k+ record log stream.
* **Processing Engine:** Distributed Spark Cluster (Local Mode).
* **Storage Format:** Columnar **Parquet** for optimized analytical performance.

## Security Logic
The analyzer implements heuristic patterns to detect:
1. **SQL Injection:** Identifying `DROP` or `SELECT` keywords in URL parameters.
2. **Brute Force/Unauthorized Access:** Monitoring `401 Unauthorized` status codes at scale.
3. **Reconnaissance:** Detecting `Nmap` and other network scanners via User-Agent analysis.

## Key Achievements
- Processed 100,000+ entries using **vectorized Spark functions**.
- Implemented **Parquet storage**, reducing data footprint and improving query speed for threat audits.
- Developed a modular architecture compatible with **AWS EMR** or **Databricks**.
