# # # # from utils.spark_session import create_spark_session
# # # # from azure.storage.blob import BlobServiceClient
# # # # import os

# # # # spark = create_spark_session("Check-Silver-Size")

# # # # # Le chemin vers tes dossiers sociaux dans Silver 
# # # # # (tu peux aussi utiliser l'URL directe si tu as configuré les clés dans Spark)
# # # # chemin_silver_social = "chemin/vers/ton/dossier/telecharge/silver/social/*/*/*/*.jsonl"

# # # # print("🔍 Lecture de la couche Silver en cours...")
# # # # df_silver = spark.read.json(chemin_silver_social)

# # # # total_lignes = df_silver.count()
# # # # total_uniques = df_silver.dropDuplicates(["article_id"]).count()

# # # # print(f"📊 Total des lignes dans Silver : {total_lignes}")
# # # # print(f"💎 Total des articles uniques : {total_uniques}")

# # # # from azure.storage.blob import BlobServiceClient
# # # # import os, json
# # # # from dotenv import load_dotenv

# # # # load_dotenv()

# # # # client = BlobServiceClient(
# # # #     account_url=f"https://{os.getenv('AZURE_STORAGE_ACCOUNT')}.blob.core.windows.net",
# # # #     credential=os.getenv("AZURE_STORAGE_KEY")
# # # # )

# # # # def count_records(container_name, prefix):
# # # #     container = client.get_container_client(container_name)
# # # #     blobs = list(container.list_blobs(name_starts_with=prefix))
    
# # # #     total_lines = 0
# # # #     total_blobs = 0
# # # #     errors = 0
    
# # # #     for blob in blobs[:50]:  # sample sur 50 blobs pour diagnostic rapide
# # # #         content = container.download_blob(blob.name).readall().decode("utf-8")
# # # #         for line in content.strip().split("\n"):
# # # #             if line.strip():
# # # #                 try:
# # # #                     json.loads(line)
# # # #                     total_lines += 1
# # # #                 except:
# # # #                     errors += 1
# # # #         total_blobs += 1
    
# # # #     return total_blobs, total_lines, errors

# # # # print("=== DIAGNOSTIC Bronze Social (sample 50 blobs) ===")
# # # # blobs, lines, errors = count_records("bronze-crowdquant", "social/")
# # # # print(f"Blobs lus     : {blobs}")
# # # # print(f"Lignes valides: {lines}")
# # # # print(f"Lignes erreurs: {errors}")
# # # # print(f"Taux d'erreur : {errors/(lines+errors)*100:.1f}%") 

# # # from azure.storage.blob import BlobServiceClient
# # # import os, json
# # # from dotenv import load_dotenv
# # # load_dotenv()

# # # client = BlobServiceClient(
# # #     account_url=f"https://{os.getenv('AZURE_STORAGE_ACCOUNT')}.blob.core.windows.net",
# # #     credential=os.getenv("AZURE_STORAGE_KEY")
# # # )

# # # # 1. Compter TOUS les blobs Bronze social
# # # bronze = client.get_container_client("bronze-crowdquant")
# # # all_blobs = list(bronze.list_blobs(name_starts_with="social/"))
# # # print(f"Total blobs Bronze social : {len(all_blobs)}")
# # # print(f"Estimation lignes totales : {len(all_blobs) * 46:,}")

# # # # 2. Lire ton silver_state.json
# # # silver = client.get_container_client("silver-crowdquant")
# # # state_blob = silver.download_blob("silver_state.json").readall()
# # # state = json.loads(state_blob)
# # # print(f"\nContenu silver_state.json :")
# # # print(json.dumps(state, indent=2))



# # from azure.storage.blob import BlobServiceClient
# # import os
# # from dotenv import load_dotenv
# # load_dotenv()

# # client = BlobServiceClient(
# #     account_url=f"https://{os.getenv('AZURE_STORAGE_ACCOUNT')}.blob.core.windows.net",
# #     credential=os.getenv("AZURE_STORAGE_KEY")
# # )

# # silver = client.get_container_client("silver-crowdquant")
# # blobs = list(silver.list_blobs())
# # print(f"Blobs dans Silver : {len(blobs)}")
# # for b in blobs[:20]:
# #     print(f"  {b.name}  ({b.size} bytes)")


# from azure.storage.blob import BlobServiceClient
# from dotenv import load_dotenv
# import os

# load_dotenv()

# client = BlobServiceClient(
#     account_url=f"https://{os.getenv('AZURE_STORAGE_ACCOUNT')}.blob.core.windows.net",
#     credential=os.getenv("AZURE_STORAGE_KEY")
# )

# silver = client.get_container_client("silver-crowdquant")

# excluded_prefixes = (
#     "_$azuretmpfolder$",
#     "_temporary",
#     ".",
# )

# def is_final_blob(name):
#     parts = name.split("/")
#     if not name or name.endswith("/"):
#         return False
#     if any(p in name for p in excluded_prefixes):
#         return False
#     if "/_temporary/" in name or name.startswith("_temporary/"):
#         return False
#     if name.split("/")[-1].startswith("_"):
#         return False
#     return True

# folders = {}
# total_count = 0
# total_size = 0

# for blob in silver.list_blobs():
#     if not is_final_blob(blob.name):
#         continue
#     root = blob.name.split("/")[0]
#     folders.setdefault(root, {"count": 0, "size": 0})
#     folders[root]["count"] += 1
#     folders[root]["size"] += blob.size
#     total_count += 1
#     total_size += blob.size

# print("=== Silver final uniquement ===")
# for folder, stats in sorted(folders.items()):
#     print(f"{folder:20} -> {stats['count']:6} blobs | {stats['size'] / (1024**2):.2f} MB")

# print("-" * 60)
# print(f"TOTAL               -> {total_count:6} blobs | {total_size / (1024**2):.2f} MB")




from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

load_dotenv()

client = BlobServiceClient(
    account_url=f"https://{os.getenv('AZURE_STORAGE_ACCOUNT')}.blob.core.windows.net",
    credential=os.getenv("AZURE_STORAGE_KEY")
)

silver = client.get_container_client("silver-crowdquant")

for blob in silver.list_blobs():
    print(blob.name, blob.size)