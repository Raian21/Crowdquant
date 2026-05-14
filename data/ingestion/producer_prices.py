import time
import requests
import json
import logging
import os
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import uuid

# Chargement des variables d'environnement (.env non versionné)
load_dotenv()

# Configuration des logs pour un affichage propre dans le terminal
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration Kafka via .env (avec valeurs par défaut)
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_PRICES", "prices_raw")

INTERVAL_SECONDS = 60  # Appel toutes les 60 secondes pour éviter l'erreur 429 (Rate Limit)

def create_producer():
    """Initialise le producteur Kafka"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        logger.info(f"✅ Connexion à Kafka réussie (Broker: {KAFKA_BROKER}) !")
        return producer
    except Exception as e:
        logger.error(f"❌ Erreur de connexion à Kafka : {e}")
        return None

def fetch_and_send(producer):
    """Récupère les prix sur CoinGecko et les envoie à Kafka"""
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_vol=true&include_market_cap=true&include_24hr_change=true&include_last_updated_at=true"
    #url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_vol=true"
    logger.info(f"🚀 Démarrage de l'ingestion temps réel vers '{KAFKA_TOPIC}'...")

    try:
        while True:
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                
                # Le schéma CrowdQuant définitif (Plat + Traçabilité + Raw_json)
                message = {
                    "schema_version": "1.1", # Version du schéma pour gérer les évolutions futures
                    "event_id": str(uuid.uuid4()), # Génère un identifiant unique (ex: a1b2c3d4-...)
                    "timestamp_ingest": datetime.utcnow().isoformat() + "Z", # Le "Z" indique formellement l'UTC
                    "timestamp_api": datetime.utcfromtimestamp(
                        data["bitcoin"].get("last_updated_at", datetime.utcnow().timestamp())
                    ).isoformat() + "Z",
                    "source": "coingecko",
                    "asset": "BTC",
                    "currency": "USD",
                    "price_usd": data["bitcoin"]["usd"],
                    "volume_24h": round(data["bitcoin"]["usd_24h_vol"], 2),
                    "market_cap_usd": round(data["bitcoin"]["usd_market_cap"], 2),
                    "price_change_pct_24h": round(data["bitcoin"]["usd_24h_change"], 3),
                    #"raw_json": data # La preuve brute est conservée !
                }
            
            # if response.status_code == 200:
            #     data = response.json()
                
            #     # Formatage du message pour la Zone Bronze (Raw)
            #     message = {
            #         "timestamp": datetime.utcnow().isoformat(), # Standardisation en UTC
            #         "source": "coingecko",                      # Traçabilité de la source
            #         "symbol": "BTC",
            #         "price_usd": data["bitcoin"]["usd"],
            #         "volume_24h": round(data["bitcoin"]["usd_24h_vol"], 2)
            #     }
                
                # Envoi du message dans Kafka
                producer.send(KAFKA_TOPIC, value=message)
                producer.flush() # Force l'envoi immédiat
                
                logger.info(f"📤 Donnée injectée : {message['price_usd']}$ | Vol: {message['volume_24h']}")
                
                # Pause normale
                time.sleep(INTERVAL_SECONDS) 
                
            else:
                logger.warning(f"⚠️ Erreur API CoinGecko : {response.status_code}. Mise en pause de 2 minutes pour éviter le ban IP.")
                # Backoff : On attend plus longtemps en cas d'erreur API
                time.sleep(120) 
                
    except KeyboardInterrupt:
        logger.info("\n🛑 Arrêt manuel du producteur (Ctrl+C).")
    except Exception as e:
        logger.error(f"❌ Erreur inattendue lors de l'ingestion : {e}")
    finally:
        if producer:
            producer.close()
            logger.info("Producer fermé proprement.")

if __name__ == "__main__":
    kafka_producer = create_producer()
    if kafka_producer:
        fetch_and_send(kafka_producer)