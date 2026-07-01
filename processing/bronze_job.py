import os
import json
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, year, month, day, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from azure.storage.blob import BlobServiceClient
import os
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
import sys
import os

# Forcer Spark à utiliser le Python de ton .venv
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# (La suite de ton code avec load_dotenv() et SparkSession...)

load_dotenv()

os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

# ─── CONFIG ───────────────────────────────────────────────────────
KAFKA_BROKER         = os.getenv("KAFKA_BROKER", "localhost:9092")
AZURE_CONNECTION_STR = os.getenv("AZURE_CONNECTION_STRING")
AZURE_CONTAINER      = os.getenv("AZURE_CONTAINER_BRONZE", "bronze")
LOCAL_BASE           = "file:///C:/Users/HP/Documents/ProgPython/BasesPython/work/Crowdquant/data_local/bronze"
MAX_OFFSETS          = 5000

# ─── SCHÉMAS ──────────────────────────────────────────────────────
schema_prices = StructType([
    StructField("schema_version",       StringType(), True),
    StructField("event_id",             StringType(), True),
    StructField("timestamp_ingest",     StringType(), True),
    StructField("timestamp_api",        StringType(), True),
    StructField("source",               StringType(), True),
    StructField("asset",                StringType(), True),
    StructField("currency",             StringType(), True),
    StructField("price_usd",            DoubleType(), True),
    StructField("volume_24h",           DoubleType(), True),
    StructField("market_cap_usd",       DoubleType(), True),
    StructField("price_change_pct_24h", DoubleType(), True),
    StructField("price_open",           DoubleType(), True),
    StructField("price_high",           DoubleType(), True),
    StructField("price_low",            DoubleType(), True),
])

schema_social = StructType([
    StructField("schema_version",      StringType(), True),
    StructField("content_type",        StringType(), True),
    StructField("relevance_hint",      StringType(), True),
    StructField("article_id",          StringType(), True),
    StructField("timestamp_ingestion", StringType(), True),
    StructField("timestamp_creation",  StringType(), True),
    StructField("source",              StringType(), True),
    StructField("source_name",         StringType(), True),
    StructField("title",               StringType(), True),
    StructField("text",                StringType(), True),
    StructField("url",                 StringType(), True),
    StructField("categories",          StringType(), True),
    StructField("relevance_hint",      StringType(), True),
    StructField("crypto_symbol",       StringType(), True),
])

# ─── SPARK ────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("CrowdQuant-Bronze-DualWrite") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark démarré — Dual Write (Local earliest + Azure latest)")

# ─── AZURE CLIENT ─────────────────────────────────────────────────
blob_service  = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STR)
container_cli = blob_service.get_container_client(AZURE_CONTAINER)

# ─── HELPER AZURE ─────────────────────────────────────────────────
def write_to_azure(df, epoch_id, topic_name):
    if df.rdd.isEmpty():
        return
    rows = df.collect()
    dt   = datetime.utcnow()
    partition = (
        f"{topic_name}/year={dt.year}/"
        f"month={dt.month:02d}/day={dt.day:02d}/"
        f"hour={dt.hour:02d}"
    )
    blob_name = f"{partition}/batch_{epoch_id}_{dt.strftime('%H%M%S')}.jsonl"
    ndjson    = "\n".join(
        json.dumps(r.asDict(), ensure_ascii=False, default=str)
        for r in rows
    )
    try:
        container_cli.upload_blob(
            name=blob_name,
            data=ndjson.encode("utf-8"),
            overwrite=True
        )
        print(f"☁️  Azure  → {blob_name} ({len(rows)} msgs)")
    except Exception as e:
        print(f"❌ Azure erreur : {e}")

# ─── STREAMS LOCAL (earliest — aspire tout l'historique) ──────────
def read_local(topic, schema):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", MAX_OFFSETS) \
        .option("failOnDataLoss", "false") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("d")) \
        .select("d.*") \
        .withColumn("year",  year(current_timestamp())) \
        .withColumn("month", month(current_timestamp())) \
        .withColumn("day",   day(current_timestamp()))

# ─── STREAMS AZURE (latest — nouveaux messages uniquement) ────────
def read_azure(topic, schema):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetsPerTrigger", MAX_OFFSETS) \
        .option("failOnDataLoss", "false") \
        .load() \
        .select(from_json(col("value").cast("string"), schema).alias("d")) \
        .select("d.*")

# ─── LANCEMENT PRICES ─────────────────────────────────────────────
df_local_prices = read_local("prices_raw", schema_prices)
df_azure_prices = read_azure("prices_raw", schema_prices)

query_local_prices = df_local_prices.writeStream \
    .format("json") \
    .partitionBy("year", "month", "day") \
    .option("path", f"{LOCAL_BASE}/prices") \
    .option("checkpointLocation",
            "./checkpoints/local/prices") \
    .trigger(processingTime="60 seconds") \
    .start()

query_azure_prices = df_azure_prices.writeStream \
    .foreachBatch(
        lambda df, eid: write_to_azure(df, eid, "prices")
    ) \
    .option("checkpointLocation",
            "./checkpoints/azure/prices") \
    .trigger(processingTime="60 seconds") \
    .start()

print("✅ Prices — Local (earliest) + Azure (latest)")

# ─── LANCEMENT SOCIAL ─────────────────────────────────────────────
df_local_social = read_local("btc-social", schema_social)
df_azure_social = read_azure("btc-social", schema_social)

query_local_social = df_local_social.writeStream \
    .format("json") \
    .partitionBy("year", "month", "day") \
    .option("path", f"{LOCAL_BASE}/social") \
    .option("checkpointLocation",
            "./checkpoints/local/social") \
    .trigger(processingTime="60 seconds") \
    .start()

query_azure_social = df_azure_social.writeStream \
    .foreachBatch(
        lambda df, eid: write_to_azure(df, eid, "social")
    ) \
    .option("checkpointLocation",
            "./checkpoints/azure/social") \
    .trigger(processingTime="60 seconds") \
    .start()

print("✅ Social — Local (earliest) + Azure (latest)")
print("📡 Dual Write actif...")

spark.streams.awaitAnyTermination()