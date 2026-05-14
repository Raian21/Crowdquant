import json
import time
import uuid
import os
import requests
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
KAFKA_BROKER        = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_SOCIAL        = os.getenv("KAFKA_TOPIC_SOCIAL", "social_raw")   # Topic news/sentiment
TOPIC_MARKET        = os.getenv("KAFKA_TOPIC_PRICES", "prices_raw")   # Topic prix marché
CRYPTOCOMPARE_KEY   = os.getenv("CRYPTOCOMPARE_KEY")                   # Clé gratuite sur cryptocompare.com
INTERVAL_SECONDS    = 60
CRYPTO_SYMBOL       = "BTC"
CURRENCY            = "USD"

BASE_URL = "https://min-api.cryptocompare.com/data"

HEADERS = {
    "authorization": f"Apikey {CRYPTOCOMPARE_KEY}"
}

# Initialisation du Producer Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def get_relevance_hint(title: str, text: str, categories: str = "") -> str:
    """
    Classe l'article en 3 niveaux de pertinence pour filtrer le bruit.
    """
    content = f"{title} {text} {categories}".lower()
    
    # Niveau 1 : Directement lié au Bitcoin
    btc_keywords = ["bitcoin", "btc", "satoshi", "halving"]
    if any(kw in content for kw in btc_keywords):
        return "btc_direct"
        
    # Niveau 2 : Crypto en général ou Blockchain
    crypto_keywords = ["crypto", "blockchain", "ethereum", "eth", "web3", "stablecoin", "usdc"]
    if any(kw in content for kw in crypto_keywords):
        return "crypto_general"
        
    # Niveau 3 : Bruit (Tech, IA, Finance traditionnelle sans lien clair)
    return "weak_relation"


def fetch_crypto_news() -> list[dict]:
    """
    Récupère les dernières actualités crypto depuis CryptoCompare News.
    Ces articles sont une source de sentiment complémentaire à NewsAPI/GDELT.
    """
    articles = []
    url = f"{BASE_URL}/v2/news/"

    params = {
        "categories": "BTC,Blockchain,Mining",
        "excludeCategories": "Sponsored",
        "sortOrder": "latest",
        "lang": "EN"
    }

    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        for article in data.get("Data", []):
            
            # 1. On prépare les variables
            title_text = article.get("title", "")
            body_text = article.get("body", "")[:500]
            cat_text = article.get("categories", "")

            articles.append({
                # --- LA SIGNATURE 1.1 ---
                "schema_version": "1.1", 
                "content_type": "news_article",
                "relevance_hint": get_relevance_hint(title_text, body_text, cat_text),
                
                "article_id": str(article.get("id", "")),
                "timestamp_ingestion": datetime.utcnow().isoformat() + "Z",
                "timestamp_creation": datetime.utcfromtimestamp(article.get("published_on", 0)).isoformat() + "Z",
                "source": "cryptocompare_news",
                "source_name": article.get("source", ""),
                "title": title_text,
                "text": body_text,
                "url": article.get("url", ""),
                "categories": cat_text,
                "crypto_symbol": CRYPTO_SYMBOL
            })
            
            # articles.append({
            #     # --- LA SIGNATURE DE L'ARCHITECTE ---
            #     "schema_version": "1.1", 
            #     "content_type": "news_article",
            #     "relevance_hint": get_relevance_hint(title_text, body_text, cat_text),
                
            #     "article_id": str(article.get("id", "")),
            #     # Ajout du "Z" pour forcer l'UTC sur l'heure de ton script
            #     "timestamp_ingestion": datetime.utcnow().isoformat() + "Z",
            #     # Ajout du "Z" pour forcer l'UTC sur l'heure de CryptoCompare
            #     "timestamp_creation": datetime.utcfromtimestamp(article.get("published_on", 0)).isoformat() + "Z",
            #     "source": "cryptocompare_news",
            #     "source_name": article.get("source", ""),
            #     "title": article.get("title", ""),
            #     "text": article.get("body", "")[:500],
            #     "url": article.get("url", ""),
            #     "categories": article.get("categories", ""),
            #     "crypto_symbol": CRYPTO_SYMBOL # (Assure-toi que CRYPTO_SYMBOL = "BTC" est bien défini plus haut)
            # })

        # for article in data.get("Data", []):
        #     articles.append({
        #         "article_id": str(article.get("id", "")),
        #         "timestamp_ingestion": datetime.utcnow().isoformat(),
        #         "timestamp_creation": datetime.utcfromtimestamp(
        #             article.get("published_on", 0)
        #         ).isoformat(),
        #         "source": "cryptocompare_news",
        #         "source_name": article.get("source", ""),
        #         "title": article.get("title", ""),
        #         "text": article.get("body", "")[:500],
        #         "url": article.get("url", ""),
        #         "categories": article.get("categories", ""),
        #         "crypto_symbol": CRYPTO_SYMBOL
        #     })

    except requests.exceptions.RequestException as e:
        print(f"Erreur requête CryptoCompare News : {e}")

    return articles


def fetch_crypto_price() -> dict | None:
    """
    Récupère le prix spot actuel du BTC en USD via CryptoCompare.
    Ce document est envoyé dans le topic marché (market_raw), pas social_raw.
    """
    url = f"{BASE_URL}/price"
    params = {
        "fsym": CRYPTO_SYMBOL,
        "tsyms": CURRENCY
    }

    try:
        response = requests.get(url, params=params, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()

        return {
            "schema_version": "1.3",     # Version du schéma pour gérer les évolutions futures   
            "event_id": str(uuid.uuid4()),
            "timestamp_ingest": datetime.utcnow().isoformat() + "Z",
            "timestamp_api": datetime.utcnow().isoformat() + "Z", # CryptoCompare Spot ne donne pas l'heure API, on utilise l'heure actuelle
            "source": "cryptocompare_price",
            "asset": CRYPTO_SYMBOL,
            "currency": CURRENCY,
            "price_usd": data.get(CURRENCY),
            "volume_24h": None,  # CryptoCompare price endpoint ne donne pas le volume
            #"source": "cryptocompare_price"
        }

    except requests.exceptions.RequestException as e:
        print(f"Erreur requête CryptoCompare Price : {e}")
        return None


print(f"Producer CryptoCompare démarré — topics : {TOPIC_SOCIAL} | {TOPIC_MARKET}")

while True:
    try:
        # 1. Actualités → topic social_raw (sentiment)
        articles = fetch_crypto_news()
        if articles:
            for article in articles:
                producer.send(TOPIC_SOCIAL, value=article)
            producer.flush()
            print(f"[{datetime.utcnow().isoformat()}] {len(articles)} news CryptoCompare → {TOPIC_SOCIAL}")

        # 2. Prix spot → topic market_raw
        price = fetch_crypto_price()
        if price:
            producer.send(TOPIC_MARKET, value=price)
            producer.flush()
            print(f"[{datetime.utcnow().isoformat()}] Prix BTC : {price['price_usd']} USD → {TOPIC_MARKET}")

    except Exception as e:
        print(f"Erreur d'ingestion CryptoCompare : {e}")

    time.sleep(INTERVAL_SECONDS)
