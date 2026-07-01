import sys
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os

from utils.spark_session import create_spark_session
from utils.silver_state import load_state, save_state, get_new_blobs
from transformations.prices_silver import (
    read_bronze_prices,
    transform_prices_to_silver,
    write_prices_silver
)
from transformations.social_silver import (
    read_bronze_social,
    transform_social_to_silver,
    write_social_silver
)

load_dotenv()

AZURE_ACCOUNT        = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_KEY            = os.getenv("AZURE_STORAGE_KEY")
AZURE_CONTAINER_BRONZE = os.getenv("AZURE_CONTAINER_BRONZE", "bronze-crowdquant")


def main():
    spark = create_spark_session("CrowdQuant-Silver-Azure")

    # Client Bronze pour lister les blobs
    bronze_client = BlobServiceClient(
        account_url=f"https://{AZURE_ACCOUNT}.blob.core.windows.net",
        credential=AZURE_KEY
    ).get_container_client(AZURE_CONTAINER_BRONZE)

    # Charge l'état du dernier run
    state = load_state()

    try:
        print("\n🔹 Début du traitement Silver CrowdQuant (incrémental)")

        # ── PRICES ────────────────────────────────────────────────
        print("\n⚡ Prices : détection des nouveaux blobs Bronze...")
        prices_blobs = get_new_blobs(
            bronze_client,
            prefix="prices/",
            last_blob=state.get("prices_last_blob")
        )

        if prices_blobs:
            bronze_prices_df  = read_bronze_prices(spark, prices_blobs)
            silver_prices_df  = transform_prices_to_silver(bronze_prices_df, spark)
            write_prices_silver(silver_prices_df)

            # Met à jour l'état avec le dernier blob traité
            state["prices_last_blob"] = prices_blobs[-1].name
            print(f"✅ Prices Silver terminé")
        else:
            print("✅ Prices Silver déjà à jour — rien à faire")

        # ── SOCIAL ────────────────────────────────────────────────
        print("\n⚡ Social : détection des nouveaux blobs Bronze...")
        social_blobs = get_new_blobs(
            bronze_client,
            prefix="social/",
            last_blob=state.get("social_last_blob")
        )

        if social_blobs:
            # Traitement par lots de 50 blobs max pour éviter l'OOM
            BATCH_SIZE = 50
            total_records = 0

            for i in range(0, len(social_blobs), BATCH_SIZE):
                batch = social_blobs[i:i + BATCH_SIZE]
                print(f"\n   Lot {i//BATCH_SIZE + 1} : {len(batch)} blobs")

                bronze_social_df = read_bronze_social(spark, batch)
                if bronze_social_df.rdd.isEmpty():
                    continue

                silver_social_df = transform_social_to_silver(
                    bronze_social_df, spark
                )
                write_social_silver(silver_social_df)
                total_records += silver_social_df.count()

            # Met à jour l'état avec le dernier blob traité
            state["social_last_blob"] = social_blobs[-1].name
            print(f"✅ Social Silver terminé — {total_records} records")
        else:
            print("✅ Social Silver déjà à jour — rien à faire")

        # Sauvegarde l'état SEULEMENT si tout s'est bien passé
        save_state(state)
        print("\n🚀 Silver terminé avec succès.")

    except Exception as e:
        print(f"\n❌ Erreur : {e}", file=sys.stderr)
        print("⚠️  État NON sauvegardé — le prochain run reprendra depuis le même point")
        raise

    finally:
        spark.stop()
        print("🛑 Session Spark arrêtée.")


if __name__ == "__main__":
    main()