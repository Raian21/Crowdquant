import os
import json
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, ContainerClient

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

load_dotenv()

AZURE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_CONTAINER_BRONZE = os.getenv("AZURE_CONTAINER_BRONZE", "bronze-crowdquant")
AZURE_CONTAINER_SILVER = os.getenv("AZURE_CONTAINER_SILVER", "silver-crowdquant")
AZURE_KEY = os.getenv("AZURE_STORAGE_KEY")

BRONZE_PRICES_SCHEMA = StructType([
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

def get_container_client(container_name: str) -> ContainerClient:
    client = BlobServiceClient(
        account_url=f"https://{AZURE_ACCOUNT}.blob.core.windows.net",
        credential=AZURE_KEY
    )
    return client.get_container_client(container_name)


def read_bronze_prices(spark, blobs: list) -> DataFrame:
    """
    Lit uniquement les blobs passés en paramètre.
    """
    import json
    from azure.storage.blob import BlobServiceClient

    client = BlobServiceClient(
        account_url=f"https://{AZURE_ACCOUNT}.blob.core.windows.net",
        credential=os.getenv("AZURE_STORAGE_KEY")
    )
    container = client.get_container_client(AZURE_CONTAINER_BRONZE)

    if not blobs:
        print("⚠️ Aucun nouveau blob prices à traiter.")
        return spark.createDataFrame([], schema=BRONZE_PRICES_SCHEMA)

    all_rows = []
    for blob in blobs:
        content = container.download_blob(blob.name).readall().decode("utf-8")
        for line in content.strip().split("\n"):
            if line.strip():
                try:
                    all_rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    print(f"📥 {len(all_rows)} records Bronze prices à transformer")
    return spark.createDataFrame(all_rows, schema=BRONZE_PRICES_SCHEMA)

# def read_bronze_prices(spark) -> DataFrame:
    container = get_container_client(AZURE_CONTAINER_BRONZE)
    blobs = list(container.list_blobs(name_starts_with="prices/"))
    
    if not blobs:
        print("⚠️ Aucun fichier Bronze prices sur Azure.")
        return spark.createDataFrame([], schema=BRONZE_PRICES_SCHEMA)

    all_rows = []
    for blob in blobs:
        content = container.download_blob(blob.name).readall().decode("utf-8")
        for line in content.strip().split("\n"):
            if line:
                try:
                    all_rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    print(f"📥 {len(all_rows)} records Bronze prices lus depuis Azure")
    return spark.createDataFrame(all_rows, schema=BRONZE_PRICES_SCHEMA)

def transform_prices_to_silver(df: DataFrame, spark) -> DataFrame:
    if df.rdd.isEmpty():
        return df

    df.createOrReplaceTempView("bronze_prices")

    silver = spark.sql("""
        -- ÉTAPE 1 : Préparation de base
        WITH base_prepared AS (
            SELECT *,
                to_timestamp(COALESCE(timestamp_api, timestamp_ingest)) AS event_time,
                to_timestamp(timestamp_ingest) AS ingest_time,
                lower(source) AS clean_source,
                COALESCE(
                    event_id,
                    sha2(CONCAT_WS('||', source, timestamp_api, asset, CAST(price_usd AS STRING)), 256)
                ) AS dedupe_key
            FROM bronze_prices
            WHERE price_usd IS NOT NULL
        ),
        
        -- ÉTAPE 2 : Déduplication
        deduplicated AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY dedupe_key 
                    ORDER BY ingest_time DESC
                ) AS rn
            FROM base_prepared
        )
        
        -- ÉTAPE 3 : Rendu Final
        SELECT
            schema_version,
            event_id,
            event_time,
            ingest_time,
            clean_source     AS source,
            upper(asset)     AS asset,
            upper(currency)  AS currency,
            price_usd,
            
            CASE WHEN volume_24h = 0.0 THEN NULL ELSE volume_24h END AS volume_24h,
            CASE WHEN market_cap_usd = 0.0 THEN NULL ELSE market_cap_usd END AS market_cap_usd,
            
            price_change_pct_24h, price_open, price_high, price_low,
            
            (price_usd IS NOT NULL AND event_time IS NOT NULL AND clean_source IS NOT NULL) AS is_complete_record,
            
            CASE
                WHEN price_usd IS NOT NULL AND volume_24h IS NOT NULL AND market_cap_usd IS NOT NULL AND market_cap_usd != 0.0 THEN 'high'
                WHEN price_usd IS NOT NULL THEN 'medium'
                ELSE 'low'
            END AS record_quality,
            
            date_format(event_time, 'yyyy') AS year,
            date_format(event_time, 'MM')   AS month,
            date_format(event_time, 'dd')   AS day
        FROM deduplicated
        WHERE rn = 1
    """)
    return silver

def write_prices_silver(df: DataFrame) -> None:
    if df.rdd.isEmpty():
        print("⚠️ Aucun enregistrement prices_silver à écrire.")
        return

    try:
        container = get_container_client(AZURE_CONTAINER_SILVER)
    except Exception as e:
        print(f"❌ Erreur Azure : {e}")
        return

    rows = df.collect()
    dt = datetime.utcnow()

    partition = f"prices/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    blob_name = f"{partition}/silver_{dt.strftime('%H%M%S')}.jsonl"

    ndjson = "\n".join(json.dumps(row.asDict(), ensure_ascii=False, default=str) for row in rows)

    try:
        container.upload_blob(name=blob_name, data=ndjson.encode("utf-8"), overwrite=True)
        print(f"☁️  Silver prices → {blob_name} ({len(rows)} records)")
    except Exception as e:
        print(f"❌ Erreur Azure Silver prices : {e}")