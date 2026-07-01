from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import json

load_dotenv()

def get_blob_client():
    return BlobServiceClient.from_connection_string(
        os.getenv("AZURE_CONNECTION_STRING")
    )

def write_to_bronze(data: dict, topic: str, timestamp: str):
    """
    Écrit un message JSON dans Azure Blob Storage Bronze.
    Structure : bronze/{topic}/year=YYYY/month=MM/day=DD/hour=HH/{timestamp}.json
    """
    client = get_blob_client()
    container = client.get_container_client(
        os.getenv("AZURE_CONTAINER_BRONZE", "bronze")
    )

    # Partitionnement temporel pour optimiser les lectures Spark
    from datetime import datetime
    dt = datetime.utcnow()
    blob_path = (
        f"{topic}/"
        f"year={dt.year}/"
        f"month={dt.month:02d}/"
        f"day={dt.day:02d}/"
        f"hour={dt.hour:02d}/"
        f"{timestamp}.json"
    )

    container.upload_blob(
        name=blob_path,
        data=json.dumps(data, ensure_ascii=False),
        overwrite=True
    )
    return blob_path