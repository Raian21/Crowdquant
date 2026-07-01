import os
import json
from datetime import datetime
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient, ContainerClient

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType

load_dotenv()

AZURE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_CONTAINER_BRONZE = os.getenv("AZURE_CONTAINER_BRONZE", "bronze-crowdquant")
AZURE_CONTAINER_SILVER = os.getenv("AZURE_CONTAINER_SILVER", "silver-crowdquant")
AZURE_KEY = os.getenv("AZURE_STORAGE_KEY")

BRONZE_SOCIAL_SCHEMA = StructType([
    StructField("schema_version",      StringType(), True),
    StructField("article_id",          StringType(), True),
    StructField("timestamp_ingestion", StringType(), True),
    StructField("timestamp_creation",  StringType(), True),
    StructField("source",              StringType(), True),
    StructField("source_name",         StringType(), True),
    StructField("content_type",        StringType(), True),
    StructField("title",               StringType(), True),
    StructField("text",                StringType(), True),
    StructField("url",                 StringType(), True),
    StructField("categories",          StringType(), True),
    StructField("crypto_symbol",       StringType(), True),
    StructField("relevance_hint",      StringType(), True),
    StructField("language",            StringType(), True),
    StructField("country",             StringType(), True),
])

def get_container_client(container_name: str) -> ContainerClient:
    client = BlobServiceClient(
        account_url=f"https://{AZURE_ACCOUNT}.blob.core.windows.net",
        credential=AZURE_KEY
    )
    return client.get_container_client(container_name)

def read_bronze_social(spark, blobs: list) -> DataFrame:
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
        print("⚠️ Aucun nouveau blob socials à traiter.")
        return spark.createDataFrame([], schema=BRONZE_SOCIAL_SCHEMA)

    all_rows = []
    for blob in blobs:
        content = container.download_blob(blob.name).readall().decode("utf-8")
        for line in content.strip().split("\n"):
            if line.strip():
                try:
                    all_rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    print(f"📥 {len(all_rows)} records Bronze social à transformer")
    return spark.createDataFrame(all_rows, schema=BRONZE_SOCIAL_SCHEMA)

# def read_bronze_social(spark) -> DataFrame:
    container = get_container_client(AZURE_CONTAINER_BRONZE)
    blobs = list(container.list_blobs(name_starts_with="social/"))
    
    if not blobs:
        print("⚠️ Aucun fichier Bronze social sur Azure.")
        return spark.createDataFrame([], schema=BRONZE_SOCIAL_SCHEMA)

    all_rows = []
    for blob in blobs:
        content = container.download_blob(blob.name).readall().decode("utf-8")
        for line in content.strip().split("\n"):
            if line:
                try:
                    all_rows.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    print(f"📥 {len(all_rows)} records Bronze social lus depuis Azure")
    return spark.createDataFrame(all_rows, schema=BRONZE_SOCIAL_SCHEMA)

def transform_social_to_silver(df: DataFrame, spark) -> DataFrame:
    if df.rdd.isEmpty():
        return df

    df.createOrReplaceTempView("bronze_social")

    silver = spark.sql("""
        -- ÉTAPE 1 : Préparation et clés
        WITH base_prepared AS (
            SELECT *,
                CASE WHEN text IS NULL OR TRIM(text) = '' THEN title ELSE text END AS text_raw,
                COALESCE(
                    article_id, 
                    url, 
                    sha2(CONCAT_WS('||', source, title, timestamp_creation), 256)
                ) AS dedupe_key
            FROM bronze_social
            WHERE title IS NOT NULL
        ),
        
        -- ÉTAPE 2 : Nettoyage centralisé (DRY)
        text_cleaned AS (
            SELECT *,
                lower(TRIM(regexp_replace(text_raw, 'https?://\\\\S+', ''))) AS clean_text,
                (relevance_hint = 'btc_direct' 
                 OR title RLIKE '(?i)\\\\bBTC\\\\b|Bitcoin' 
                 OR text_raw RLIKE '(?i)\\\\bBTC\\\\b|Bitcoin' 
                 OR categories RLIKE '(?i)\\\\bBTC\\\\b|Bitcoin') AS is_btc_related
            FROM base_prepared
        ),
        
        -- ÉTAPE 3 : Déduplication
        deduplicated AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY dedupe_key 
                    ORDER BY timestamp_ingestion DESC
                ) AS rn
            FROM text_cleaned
        )
        
        -- ÉTAPE 4 : Rendu Final
        SELECT
            schema_version,
            article_id,
            to_timestamp(timestamp_creation)  AS event_time,
            to_timestamp(timestamp_ingestion) AS ingest_time,
            lower(source)                     AS source,
            source_name,
            content_type,
            title,
            text_raw                          AS text,
            clean_text,
            lower(TRIM(title))                AS clean_title,
            url,
            language,
            country,
            categories,
            upper(crypto_symbol)              AS analysis_scope,
            relevance_hint,
            is_btc_related,
            LENGTH(clean_text)                AS text_length,
            LENGTH(clean_text) >= 20          AS has_meaningful_text,
            CASE
                WHEN LENGTH(clean_text) >= 20 AND is_btc_related THEN 'high'
                WHEN LENGTH(clean_text) >= 20 THEN 'medium'
                ELSE 'low'
            END                               AS record_quality,
            date_format(to_timestamp(timestamp_creation), 'yyyy') AS year,
            date_format(to_timestamp(timestamp_creation), 'MM')   AS month,
            date_format(to_timestamp(timestamp_creation), 'dd')   AS day
        FROM deduplicated
        WHERE rn = 1
    """)
    return silver

def write_social_silver(df: DataFrame) -> None:
    if df.rdd.isEmpty():
        print("⚠️ Aucun enregistrement social_silver à écrire.")
        return

    try:
        container = get_container_client(AZURE_CONTAINER_SILVER)
    except Exception as e:
        print(f"❌ Erreur de connexion au conteneur : {e}")
        return

    rows = df.collect()
    dt = datetime.utcnow()

    partition = f"social/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}"
    blob_name = f"{partition}/silver_{dt.strftime('%H%M%S')}.jsonl"

    ndjson = "\n".join(json.dumps(row.asDict(), ensure_ascii=False, default=str) for row in rows)

    try:
        container.upload_blob(name=blob_name, data=ndjson.encode("utf-8"), overwrite=True)
        print(f"☁️  Silver social → {blob_name} ({len(rows)} records)")
    except Exception as e:
        print(f"❌ Erreur Upload Azure Silver social : {e}")