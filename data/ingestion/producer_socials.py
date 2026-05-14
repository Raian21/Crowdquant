import json
import time
import praw
import os
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv

# Charge les variables cachées dans le fichier .env
load_dotenv()

# --- CONFIGURATION ---
# Récupération des variables depuis le .env (avec des valeurs par défaut en cas d'oubli)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_SOCIAL", "social_raw")

INTERVAL_SECONDS = 60
SUBREDDITS = ["Bitcoin", "CryptoCurrency"]
POSTS_LIMIT = 10

# Initialisation du Producer Kafka
# value_serializer transforme notre dictionnaire Python en texte JSON lisible par Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Initialisation de l'API Reddit (PRAW)
reddit = praw.Reddit(
    client_id=os.getenv("REDDIT_CLIENT_ID"),
    client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
    user_agent="crowdquant:v1.0 (by /u/crowdquant_bot)"
)


def fetch_reddit_posts() -> list[dict]:
    """
    Parcourt les subreddits cibles et extrait les derniers posts.
    Retourne une liste de dictionnaires contenant les métadonnées de chaque post.
    """
    posts: list[dict] = []
    for sub in SUBREDDITS:
        # Récupère les X posts les plus récents (new) du subreddit
        for post in reddit.subreddit(sub).new(limit=POSTS_LIMIT):
            posts.append({
                "post_id": post.id,  # Ajout très utile pour éviter les doublons plus tard !
                # Heure où on a aspiré la donnée
                "timestamp_ingestion": datetime.utcnow().isoformat(),
                # Vraie heure de création du post
                "timestamp_creation": datetime.utcfromtimestamp(post.created_utc).isoformat(),
                "source": "reddit",
                "subreddit": sub,
                "title": post.title,
                # On limite à 500 caractères pour ne pas surcharger Kafka
                "text": post.selftext[:500] if post.selftext else "",
                "score": post.score,
                "num_comments": post.num_comments,
                "crypto_symbol": "BTC"
            })
    return posts


print(f"Producer social démarré — topic: {TOPIC}")

# Boucle infinie d'ingestion
while True:
    try:
        # 1. Extraction des données
        posts = fetch_reddit_posts()

        if not posts:
            print(f"[{datetime.utcnow().isoformat()}] Aucun post récupéré")
        else:

            # 2. Envoi message par message dans le topic Kafka
            for post in posts:
                producer.send(TOPIC, value=post)

            # 3. Forcer l'envoi immédiat du lot
            producer.flush()
            print(
                f"[{datetime.utcnow().isoformat()}] {len(posts)} posts envoyés vers {TOPIC}")

    except Exception as e:
        print(f"Erreur d'ingestion: {e}")

    # 4. Pause avant la prochaine requête pour ne pas se faire bloquer par Reddit
    time.sleep(INTERVAL_SECONDS)
