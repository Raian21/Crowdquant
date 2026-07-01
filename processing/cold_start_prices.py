import os
import uuid
from datetime import datetime
import pandas as pd

def run_cold_start_social():
    print("📥 [ETL COLD START SOCIAL] Début de l'extraction de l'historique Reddit...")
    
    # On utilise le fichier btc_historical.csv qui contient en réalité les données Reddit
    csv_path = "data/bronze/prices/btc_historical.csv" 
    
    if not os.path.exists(csv_path):
        print(f"❌ Erreur : Fichier introuvable dans : {csv_path}")
        return

    df_brut = pd.read_csv(csv_path, encoding='utf-8-sig')
    df_brut.columns = df_brut.columns.str.strip()

    print("🔄 [ETL COLD START SOCIAL] Transformation et alignement sémantique...")
    df_transformed = pd.DataFrame()
    
    # 1. Alignement strict sur le schéma social_silver
    df_transformed['schema_version'] = ["1.0.0"] * len(df_brut)
    df_transformed['article_id'] = df_brut['submission'].astype(str)
    
    # Conversion du timestamp de création historique
    df_transformed['event_time'] = pd.to_datetime(df_brut['created'], unit='s', errors='coerce').astype(str)
    df_transformed['ingest_time'] = [datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * len(df_brut)
    
    df_transformed['source'] = ["reddit"] * len(df_brut)
    df_transformed['source_name'] = df_brut['source_subreddit'].astype(str)
    df_transformed['content_type'] = ["post"] * len(df_brut)
    
    # Gestion des textes et nettoyage des valeurs manquantes (NaN)
    df_transformed['title'] = df_brut['title'].fillna("").astype(str)
    df_transformed['text'] = df_brut['selftext'].fillna("").astype(str)
    
    # Règle de fallback si le texte est vide, on prend le titre
    text_final = df_transformed['text'].where(df_transformed['text'].str.strip() != "", df_transformed['title'])
    
    # Nettoyage des URL pour le clean_text et clean_title
    df_transformed['clean_title'] = df_transformed['title'].str.replace(r'https?://\S+', '', regex=True).str.strip().str.lower()
    df_transformed['clean_text'] = text_final.str.replace(r'https?://\S+', '', regex=True).str.strip().str.lower()
    
    df_transformed['url'] = [""] * len(df_brut)
    df_transformed['language'] = ["en"] * len(df_brut) # Reddit BTC historique est majoritairement en anglais
    df_transformed['country'] = ["us"] * len(df_brut)
    df_transformed['categories'] = ["crypto, social_media"] * len(df_brut)
    df_transformed['analysis_scope'] = ["BTC"] * len(df_brut)
    df_transformed['relevance_hint'] = ["reddit_historical"] * len(df_brut)
    
    # Détection de la pertinence Bitcoin
    df_transformed['is_btc_related'] = [True] * len(df_brut)
    df_transformed['text_length'] = df_transformed['clean_text'].str.len().fillna(0).astype(int)
    df_transformed['has_meaningful_text'] = df_transformed['text_length'] >= 20
    
    # Qualité du record
    df_transformed['record_quality'] = "medium"
    
    # 2. FRACTIONNEMENT PAR LOTS DE 50 000 LIGNES MAXIMUM
    output_dir = "data/silver/social_cold_start"
    os.makedirs(output_dir, exist_ok=True)
    
    chunk_size = 50000
    print(f"📤 [ETL COLD START SOCIAL] Fractionnement par lots de {chunk_size} lignes...")
    
    for i, chunk in enumerate(range(0, len(df_transformed), chunk_size)):
        df_chunk = df_transformed.iloc[chunk:chunk + chunk_size]
        file_name = f"{output_dir}/social_history_part_{i+1}.csv"
        df_chunk.to_csv(file_name, index=False, sep=",")
        print(f" ✅ Lot {i+1} créé avec succès : {file_name} ({len(df_chunk)} lignes)")

    print("\n🚀 [SUCCESS] Le flux historique Reddit (Social) a été traité par lots avec succès !")

if __name__ == "__main__":
    run_cold_start_social()