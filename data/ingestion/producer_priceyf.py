import json
import time
import uuid
import yfinance as yf
from kafka import KafkaProducer
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC_PRICES", "crypto-prices")
INTERVAL_SECONDS = 60

# Initialisation du Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# VARIABLE GLOBALE POUR LA DÉDUPLICATION
last_sent_candle_time = None



def fetch_btc_price():
    # On isole l'appel réseau dans un bloc try/except (Gestion du Timeout)
    try:
        btc = yf.Ticker("BTC-USD")
        # On réduit la période à 1 heure (ou 1 jour si ça plante) pour éviter de télécharger trop de données
        data = btc.history(period="1d", interval="1m", timeout=10) 
        
        if data.empty:
            return None
            
        last = data.iloc[-1]
        candle_time = data.index[-1].tz_convert('UTC').isoformat().replace("+00:00", "Z")
        
        info = btc.fast_info
        volume_24h = float(info.three_month_average_volume or 0)
        
        return {
            "schema_version": "1.2", # Version du schéma pour gérer les évolutions futures
            "event_id": str(uuid.uuid4()),
            "timestamp_ingest": datetime.utcnow().isoformat() + "Z",
            "timestamp_api": candle_time, # L'heure exacte de la bougie
            "source": "yahoo_finance",
            "asset": "BTC",
            "currency": "USD",
            "price_usd": round(float(last["Close"]), 2),
            "volume_24h": volume_24h,
            "price_open": round(float(last["Open"]), 2),
            "price_high": round(float(last["High"]), 2),
            "price_low": round(float(last["Low"]), 2)
        }
    except Exception as network_error:
        print(f"⚠️ Erreur réseau/API avec YFinance : {network_error}")
        return None

print(f"🚀 Producer YFinance démarré — topic: {TOPIC}")

while True:
    try:
        record = fetch_btc_price()
        
        if record:
            current_candle_time = record["timestamp_api"]
            
            # --- LOGIQUE DE DÉDUPLICATION ---
            if current_candle_time == last_sent_candle_time:
                print(f"[{datetime.utcnow().isoformat()}] ⏳ Attente : La bougie YFinance n'a pas encore été actualisée.")
            else:
                # Nouvelle donnée ! On envoie.
                producer.send(TOPIC, value=record)
                producer.flush()
                print(f"[{record['timestamp_ingest']}] 📤 YF Injecté : {record['price_usd']} USD (Bougie de {current_candle_time})")
                
                # On met à jour la mémoire
                last_sent_candle_time = current_candle_time
        else:
            print("Données vides ou erreur, on réessaie au prochain cycle...")
            
    except Exception as e:
        print(f"Erreur inattendue dans la boucle : {e}")
        
    time.sleep(INTERVAL_SECONDS)