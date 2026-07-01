import os
import sys
from pyspark.sql import SparkSession
from dotenv import load_dotenv


def create_spark_session(app_name: str = "CrowdQuant-Silver-Azure") -> SparkSession:
    """
    Crée et configure une SparkSession locale capable de lire / écrire
    vers Azure Blob Storage via le protocole wasbs://

    Cette fonction centralise toute la configuration Spark afin d'éviter
    de dupliquer le setup dans plusieurs scripts.
    """
    load_dotenv()

    # ← AJOUTE CES DEUX LIGNES
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    azure_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    azure_key = os.getenv("AZURE_STORAGE_KEY")
    # ─── WINUTILS (Windows uniquement) ───────────────────────────────
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"

    if not azure_account:
        raise ValueError("AZURE_STORAGE_ACCOUNT est absent du fichier .env")

   

    # Configuration optionnelle pour Windows local
    # Si HADOOP_HOME est défini dans le .env, on l'ajoute à l'environnement.
    # if hadoop_home:
    #     os.environ["HADOOP_HOME"] = hadoop_home
    #     os.environ["PATH"] = os.environ.get("PATH", "") + f";{hadoop_home}\\bin"
    
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # Plus besoin de hadoop-azure ici
        .getOrCreate()
    )

    # spark = (
    #     SparkSession.builder
    #     .appName(app_name)
    #     .master("local[*]")
    #     # Ces deux options réduisent certains problèmes réseau sous Windows
    #     .config("spark.driver.host", "127.0.0.1")
    #     .config("spark.driver.bindAddress", "127.0.0.1")
    #     # Dépendances Azure / Hadoop pour accéder à Azure Blob Storage
    #     .config(
    #         "spark.jars.packages",
    #         ",".join([
    #             "org.apache.hadoop:hadoop-azure:3.3.4",
    #             "com.microsoft.azure:azure-storage:8.6.6"
    #         ])
    #     )
    #     # Authentification Azure Blob Storage
    #     .config(
    #         f"fs.azure.account.key.{azure_account}.blob.core.windows.net",
    #         azure_key
    #     )
    #     .getOrCreate()
    # )

    spark.sparkContext.setLogLevel("WARN")
    return spark