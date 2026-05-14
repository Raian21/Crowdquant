import json
import time
import os
import requests
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_SOCIAL", "social_raw")
INTERVAL_SECONDS = 900   # GDELT est mis à jour toutes les 15 min — 5 min suffit

# Mots-clés de recherche autour de Bitcoin
KEYWORDS = "Bitcoin OR BTC "

# API GDELT Doc 2.0 — entièrement gratuite, aucune clé requise
GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"


def get_relevance_hint(title: str, text: str, categories: str = "") -> str:
    """
    Classe l'article selon sa pertinence vis-à-vis du Bitcoin.
    """
    content = f"{title} {text} {categories}".lower()

    # 1. Pertinence forte
    if any(keyword in content for keyword in ["bitcoin", "btc", "satoshi", "halving"]):
        return "btc_direct"
    # 2. Pertinence moyenne (écosystème crypto)
    elif any(keyword in content for keyword in ["crypto", "blockchain", "ethereum", "etf", "web3"]):
        return "crypto_general"
    # 3. Faible pertinence (Bruit)
    else:
        return "weak_relation"


def fetch_gdelt_articles(max_retries=3) -> list[dict]:
    
    """
    Interroge l'API GDELT Doc 2.0 pour récupérer les articles liés à Bitcoin.
    GDELT est un dataset open source mondial basé sur des milliers de sources news.
    Aucune clé API requise.
    """
    
    articles = []
    params = {
        "query": KEYWORDS,
        "mode": "artlist",
        "maxrecords": 10,
        "format": "json",
        "sort": "datedesc"
        # timespan retiré — trop restrictif sur le plan gratuit
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(GDELT_URL, params=params, timeout=15)
            
            if response.status_code == 429:
                wait = 60 * (attempt + 1)  # 60s, 120s, 180s
                print(f"GDELT 429 — tentative {attempt+1}/{max_retries}, attente {wait}s")
                time.sleep(wait)
                continue  # On réessaie dans la même itération
                
            response.raise_for_status()
            data = response.json()

            for article in data.get("articles", []):
                title_text = article.get("title", "")
                body_text = title_text[:500]
                articles.append({
                    "schema_version": "1.1",
                    "content_type": "news_article",
                    "relevance_hint": get_relevance_hint(title_text, body_text),
                    "article_id": article.get("url", ""),
                    "timestamp_ingestion": datetime.utcnow().isoformat() + "Z",
                    "timestamp_creation": article.get("seendate", ""),
                    "source": "gdelt",
                    "source_name": article.get("domain", ""),
                    "title": title_text,
                    "text": body_text,
                    "url": article.get("url", ""),
                    "language": article.get("language", ""),
                    "country": article.get("sourcecountry", ""),
                    "crypto_symbol": "BTC"
                })
            return articles  # Succès — on sort immédiatement

        except requests.exceptions.RequestException as e:
            print(f"Erreur GDELT tentative {attempt+1}: {e}")
            time.sleep(30)

    print("GDELT inaccessible après 3 tentatives — on skip ce cycle")
    return articles

# def fetch_gdelt_articles() -> list[dict]:
#     """
#     Interroge l'API GDELT Doc 2.0 pour récupérer les articles liés à Bitcoin.
#     GDELT est un dataset open source mondial basé sur des milliers de sources news.
#     Aucune clé API requise.
#     """
#     articles = []

#     params = {
#         "query": KEYWORDS,
#         "mode": "artlist",         # Mode liste d'articles
#         "maxrecords": 10,          # Nombre max d'articles retournés
#         "format": "json",
#         "sort": "datedesc",       # Les plus récents en premier
#         # On peut aussi utiliser "timespan" pour limiter la fenêtre temporelle (ex: 1h, 6h, 24h)
#         "timespan": "1h"
#     }

#     try:
#         response = requests.get(GDELT_URL, params=params, timeout=15)

#         # 1. ON VÉRIFIE LE 429 EN PREMIER !
#         if response.status_code == 429:
#             retry_after = response.headers.get("Retry-After")
#             if retry_after and retry_after.isdigit():
#                 wait_time = int(retry_after)
#             else:
#                 wait_time = 60
#             print(f"⚠️ GDELT saturé (429). Attente de {wait_time}s...")
#             time.sleep(wait_time)
#             return articles

#         # 2. SEULEMENT APRÈS, ON LÈVE L'ALARME POUR LES AUTRES ERREURS
#         response.raise_for_status()
#         data = response.json()

#         for article in data.get("articles", []):
#             # 1. On prépare les variables
#             title_text = article.get("title", "")
#             # GDELT n'a pas de description, on utilise le titre tronqué
#             body_text = article.get("title", "")[:500]
#             cat_text = ""  # GDELT ne fournit pas de catégories

#             articles.append({
#                 # --- LA SIGNATURE 1.1 ---
#                 "schema_version": "1.1",
#                 "content_type": "news_article",
#                 "relevance_hint": get_relevance_hint(title_text, body_text, cat_text),

#                 "article_id": article.get("url", ""),
#                 "timestamp_ingestion": datetime.utcnow().isoformat() + "Z",
#                 "timestamp_creation": article.get("seendate", "") + "Z",
#                 "source": "gdelt",
#                 "source_name": article.get("domain", ""),
#                 "title": title_text,
#                 "text": body_text,
#                 "url": article.get("url", ""),
#                 "language": article.get("language", ""),
#                 "country": article.get("sourcecountry", ""),
#                 "crypto_symbol": "BTC"
#             })

#     except requests.exceptions.RequestException as e:
#         print(f"Erreur requête GDELT : {e}")
#     except (KeyError, ValueError) as e:
#         print(f"Erreur parsing réponse GDELT : {e}")

#     return articles


# Initialisation du Producer Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print(f"Producer GDELT démarré — topic : {TOPIC}")

while True:
    try:
        articles = fetch_gdelt_articles()

        if not articles:
            print(f"[{datetime.utcnow().isoformat()}] Aucun article GDELT récupéré")
        else:
            for article in articles:
                producer.send(TOPIC, value=article)
            producer.flush()
            print(
                f"[{datetime.utcnow().isoformat()}] {len(articles)} articles GDELT envoyés vers {TOPIC}")

    except Exception as e:
        print(f"Erreur d'ingestion GDELT : {e}")

    time.sleep(INTERVAL_SECONDS)
