import os
import time

# Set HADOOP_HOME to local hadoop bin directory to prevent PySpark errors on Windows
current_dir = os.path.dirname(os.path.abspath(__file__))
hadoop_home = os.path.join(current_dir, "hadoop")
os.environ["HADOOP_HOME"] = hadoop_home
os.environ["PATH"] += os.pathsep + os.path.join(hadoop_home, "bin")
import pandas as pd
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, when, udf
from pyspark.sql.types import StringType

# 1. Provide a mock GeoIP UDF
# In a real scenario, this would use the `geoip2` library to read a MaxMind DB.
# For demonstration without a large database, we will deterministically map the first octet of the IP to a country.
def mock_geoip_lookup(ip_address):
    if not ip_address: return "Unknown"
    
    try:
        first_octet = int(ip_address.split('.')[0])
        # Simple fake mapping
        if first_octet < 50: return "United States"
        elif first_octet < 100: return "Germany"
        elif first_octet < 150: return "China"
        elif first_octet < 200: return "Brazil"
        else: return "India"
    except:
        return "Unknown"

geoip_udf = udf(mock_geoip_lookup, StringType())

def main():
    print("Starting Distributed Log Analyzer (Streaming Mode)...")
    
    # 2. Initialize Spark Session
    # No more Colab !wget commands or paths! It runs using your local environment.
    spark = SparkSession.builder \
        .appName("RealTimeLogAnalyzer") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    # Setup directories
    input_dir = "input_logs"
    if not os.path.exists(input_dir):
        os.makedirs(input_dir)
        
    output_dir = "output"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 3. Define schema via regex
    # Reading streams as raw text first
    raw_logs_stream = spark.readStream.text(input_dir)

    # 4. Extract data using RegEx
    log_pattern = r'^(\S+) \S+ \S+ \[(.*?)\] "(.*?) (.*?) \S+" (\d+) (\d+) "(.*?)"'
    
    logs_df = raw_logs_stream.select(
        regexp_extract('value', log_pattern, 1).alias('ip'),
        regexp_extract('value', log_pattern, 4).alias('endpoint'),
        regexp_extract('value', log_pattern, 5).cast('integer').alias('status'),
        regexp_extract('value', log_pattern, 7).alias('user_agent')
    )
    
    # 5. Enrich with GeoIP and Threat Analytics
    analyzed_df = logs_df.withColumn("country", geoip_udf(col("ip"))) \
        .withColumn("threat_type",
            when(col("endpoint").contains("DROP") | col("endpoint").contains("SELECT"), "SQL_INJECTION")
            .when(col("status") == 401, "UNAUTHORIZED_ACCESS_ATTEMPT")
            .when(col("user_agent").contains("Nmap"), "NETWORK_SCANNER_DETECTED")
            .otherwise("SAFE")
        )

    # 6. Set up Aggregations for Streaming
    # We maintain a running count of threat types and countries
    threat_summary_stream = analyzed_df.groupBy("threat_type").count()
    country_summary_stream = analyzed_df.filter(col("threat_type") != "SAFE").groupBy("country", "threat_type").count()

    # 7. Start the Streaming Queries towards an Memory Sink
    # This allows us to query the data intermitently using standard Spark SQL
    query_threats = threat_summary_stream.writeStream \
        .outputMode("complete") \
        .format("memory") \
        .queryName("threats_table") \
        .start()
        
    query_countries = country_summary_stream.writeStream \
        .outputMode("complete") \
        .format("memory") \
        .queryName("countries_table") \
        .start()

    print(f"Streaming started. Tracking files in '{input_dir}'...")
    print("Will generate snapshot plots every 15 seconds. Press Ctrl+C to stop.")

    # 8. Loop to periodically query the memory tables and draw charts
    try:
        while True:
            time.sleep(15)
            # Query the in-memory streaming tables
            # They might be empty on the first few ticks if no logs are there
            try:
                pandas_threats = spark.sql("SELECT * FROM threats_table").toPandas()
                pandas_countries = spark.sql("SELECT * FROM countries_table").toPandas()
                
                if not pandas_threats.empty:
                    # Plot 1: Threat Summary Bar Chart
                    plt.figure(figsize=(10, 6))
                    # Sort for better appearance
                    df_sorted = pandas_threats.sort_values(by="count", ascending=True)
                    plt.barh(df_sorted["threat_type"], df_sorted["count"], color=['green' if x == 'SAFE' else 'red' for x in df_sorted["threat_type"]])
                    plt.title("Real-Time Threat Summary")
                    plt.xlabel("Count")
                    plt.tight_layout()
                    plt.savefig(os.path.join(output_dir, "threat_summary.png"))
                    plt.close()
                    
                    print(f"[{time.strftime('%H:%M:%S')}] Updated threat_summary.png")

                if not pandas_countries.empty:
                    # Plot 2: Threat Locations Pie Chart
                    plt.figure(figsize=(8, 8))
                    country_totals = pandas_countries.groupby("country")["count"].sum()
                    plt.pie(country_totals, labels=country_totals.index, autopct='%1.1f%%', startangle=140)
                    plt.title("Origin of Malicious Traffic")
                    plt.tight_layout()
                    plt.savefig(os.path.join(output_dir, "threat_locations.png"))
                    plt.close()
                    
                    print(f"[{time.strftime('%H:%M:%S')}] Updated threat_locations.png")

            except Exception as e:
                # Sometimes table isn't ready instantly
                pass
                
    except KeyboardInterrupt:
        print("\nStopping streaming queries...")
        query_threats.stop()
        query_countries.stop()
        
if __name__ == "__main__":
    main()