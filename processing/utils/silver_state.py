import json
import os
from datetime import datetime
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

AZURE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT")
AZURE_KEY     = os.getenv("AZURE_STORAGE_KEY")
CONTAINER     = os.getenv("AZURE_CONTAINER_SILVER", "silver-crowdquant")
STATE_BLOB    = "_state/silver_state.json"

def _get_container():
    client = BlobServiceClient(
        account_url=f"https://{AZURE_ACCOUNT}.blob.core.windows.net",
        credential=AZURE_KEY
    )
    return client.get_container_client(CONTAINER)

def load_state() -> dict:
    """
    Charge l'état Silver depuis Azure.
    Retourne {"prices_last_blob": "...", "social_last_blob": "..."}
    """
    container = _get_container()
    try:
        content = container.download_blob(STATE_BLOB).readall()
        state   = json.loads(content)
        print(f"📋 État Silver chargé : {state}")
        return state
    except Exception:
        print("📋 Aucun état Silver trouvé — premier run complet")
        return {"prices_last_blob": None, "social_last_blob": None}

def save_state(state: dict) -> None:
    """
    Sauvegarde l'état Silver sur Azure après un run réussi.
    """
    container = _get_container()
    try:
        container.upload_blob(
            name=STATE_BLOB,
            data=json.dumps(state, indent=2).encode("utf-8"),
            overwrite=True
        )
        print(f"💾 État Silver sauvegardé : {state}")
    except Exception as e:
        print(f"❌ Erreur sauvegarde état : {e}")

def get_new_blobs(container_client, prefix: str, last_blob: str) -> list:
    """
    Retourne uniquement les blobs Bronze plus récents que last_blob.
    Les blobs sont triés par nom (ordre chronologique garanti
    car les noms incluent year/month/day/hour).
    """
    all_blobs = sorted(
        container_client.list_blobs(name_starts_with=prefix),
        key=lambda b: b.name
    )

    if last_blob is None:
        # Premier run — traite tout
        new_blobs = all_blobs
    else:
        # Traite uniquement ce qui est après le dernier blob connu
        new_blobs = [b for b in all_blobs if b.name > last_blob]

    if new_blobs:
        print(f"📥 {len(new_blobs)} nouveaux blobs Bronze à traiter")
        print(f"   Premier : {new_blobs[0].name}")
        print(f"   Dernier : {new_blobs[-1].name}")
    else:
        print(f"✅ Aucun nouveau blob Bronze — Silver déjà à jour")

    return new_blobs