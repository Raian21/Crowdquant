import time
import json
import logging
from kafka import KafkaProducer
from binance.client import Client

# Configuration des logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
KAFKA_TOPIC = 'crypto_prices'
KAFKA_SERVER = 'localhost:9092'  # À adapter selon votre docker-compose
BINANCE_SYMBOL = 'BTCUSDT'

def create_producer():
    """Initialise le producteur Kafka"""
    try:
        return KafkaProducer(
            bootstrap_servers=[KAFKA_SERVER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        logger.error(f"Erreur connexion Kafka : {e}")
        return None

def fetch_and_send():
    """Récupère les prix Binance et les envoie à Kafka"""
    client = Client()  # Pas besoin d'API Key pour les données publiques de base
    producer = create_producer()
    
    if not producer:
        return

    logger.info(f"Démarrage de l'ingestion pour {BINANCE_SYMBOL}...")

    try:
        while True:
            # Récupération du prix actuel
            ticker = client.get_symbol_ticker(symbol=BINANCE_SYMBOL)
            
            data = {
                'symbol': ticker['symbol'],
                'price': float(ticker['price']),
                'timestamp': int(time.time() * 1000)
            }
            
            # Envoi vers Kafka
            producer.send(KAFKA_TOPIC, value=data)
            logger.info(f"Donnée envoyée : {data}")
            
            # Attente de 60 secondes (conformément à votre doc technique)
            time.sleep(60)
            
    except KeyboardInterrupt:
        logger.info("Arrêt du producteur.")
    finally:
        producer.close()

if __name__ == "__main__":
    fetch_and_send()