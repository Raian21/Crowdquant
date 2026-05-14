import json
import time
import os
import requests
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC          = os.getenv("KAFKA_TOPIC_SOCIAL", "social_raw")
NEWSAPI_KEY    = os.getenv("NEWSAPI_KEY")          # Clé NewsAPI Developer (gratuite)
INTERVAL_SECONDS = 900                              # Requête toutes les 15 min (plan gratuit : 100 req/jour)
KEYWORDS       = ["Bitcoin", "BTC", "crypto", "cryptocurrency"]

# Initialisation du Producer Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

BASE_URL = "https://newsapi.org/v2/everything"

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

def fetch_newsapi_articles() -> list[dict]:
    """
    Récupère les derniers articles liés à Bitcoin via NewsAPI.
    Retourne une liste de dictionnaires normalisés pour Kafka.
    """
    articles = []
    query = " OR ".join(KEYWORDS)

    params = {
        "q": query,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 20,           # 20 articles par requête
        "apiKey": NEWSAPI_KEY
    }

    try:
        response = requests.get(BASE_URL, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        for article in data.get("articles", []):
            # 1. On définit les variables AVANT de créer le dictionnaire
            title_text = article.get("title", "")
            body_text = (article.get("description") or "")[:500]
            cat_text = "" # NewsAPI n'a pas de catégories, on passe une chaîne vide
            
            articles.append({
                # --- LA SIGNATURE DE L'ARCHITECTE ---
                "schema_version": "1.1", 
                "content_type": "news_article",
                "relevance_hint": get_relevance_hint(title_text, body_text, cat_text),
                
                "article_id": article.get("url", ""),
                "timestamp_ingestion": datetime.utcnow().isoformat() + "Z", # Le Z est de retour !
                "timestamp_creation": article.get("publishedAt", ""), # NewsAPI inclut généralement déjà le Z ici
                "source": "newsapi",
                "source_name": article.get("source", {}).get("name", ""),
                "title": title_text,
                "text": body_text,
                "url": article.get("url", ""),
                "crypto_symbol": "BTC"
            })
        
        # for article in data.get("articles", []):
        #     # Normalisation du document — même structure que le producteur Reddit
        #     articles.append({
        #         # --- LA SIGNATURE DE L'ARCHITECTE ---
        #         "schema_version": "1.1", 
        #         "content_type": "news_article",
        #         "relevance_hint": get_relevance_hint(title_text, body_text, cat_text),
                
                
        #         "article_id": article.get("url", ""),          # Clé unique = URL
        #         "timestamp_ingestion": datetime.utcnow().isoformat(),
        #         "timestamp_creation": article.get("publishedAt", ""),
        #         "source": "newsapi",
        #         "source_name": article.get("source", {}).get("name", ""),
        #         "title": article.get("title", ""),
        #         # Contenu tronqué à 500 caractères (cohérence avec le producer Reddit)
        #         "text": (article.get("description") or "")[:500],
        #         "url": article.get("url", ""),
        #         "crypto_symbol": "BTC"
        #     })

    except requests.exceptions.RequestException as e:
        print(f"Erreur requête NewsAPI : {e}")

    return articles


print(f"Producer NewsAPI démarré — topic : {TOPIC}")

while True:
    try:
        articles = fetch_newsapi_articles()

        if not articles:
            print(f"[{datetime.utcnow().isoformat()}] Aucun article récupéré")
        else:
            for article in articles:
                producer.send(TOPIC, value=article)
            producer.flush()
            print(f"[{datetime.utcnow().isoformat()}] {len(articles)} articles envoyés vers {TOPIC}")

    except Exception as e:
        print(f"Erreur d'ingestion NewsAPI : {e}")

    # Respecter le quota gratuit (100 req/jour → 1 req toutes les ~14 min minimum)
    # On envoie toutes les 5 min, ce qui donne ~288 req/jour → utiliser le plan payant
    # OU augmenter l'intervalle à 900s pour rester dans le plan gratuit
    time.sleep(INTERVAL_SECONDS)
