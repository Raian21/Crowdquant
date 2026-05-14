# CrowdQuant — Kafka Producers

Ce dossier contient les scripts d'ingestion Kafka pour la plateforme CrowdQuant.
Chaque script collecte des données réelles et les pousse dans un topic Kafka.

## Structure

| Fichier | Source | Topic Kafka | Clé requise |
|---|---|---|---|
| `producer_newsapi.py` | NewsAPI | `social_raw` | Oui (gratuite) |
| `producer_gdelt.py` | GDELT Project | `social_raw` | **Non** |
| `producer_cryptocompare.py` | CryptoCompare | `social_raw` + `market_raw` | Oui (gratuite) |

## Installation

```bash
pip install kafka-python requests python-dotenv praw
```

## Configuration

Copier `.env.example` en `.env` et remplir les clés API.

```bash
cp .env.example .env
```

## Lancement

```bash
# Lancer chaque producer dans un terminal séparé
python producer_newsapi.py
python producer_gdelt.py
python producer_cryptocompare.py
```

## Format commun des documents JSON envoyés dans Kafka

Tous les producers normalisent les données dans un format identique :

```json
{
  "article_id": "identifiant unique",
  "timestamp_ingestion": "2026-05-11T17:00:00",
  "timestamp_creation": "2026-05-11T16:55:00",
  "source": "newsapi | gdelt | cryptocompare_news",
  "source_name": "CoinDesk",
  "title": "Bitcoin surges past $70,000",
  "text": "Description tronquée à 500 caractères...",
  "url": "https://...",
  "crypto_symbol": "BTC"
}
```

## Rôle de Reddit

Reddit a été retiré du pipeline principal pour des raisons de conformité
(politique données Reddit 2024+). Il peut être réintégré ultérieurement
si une autorisation explicite est obtenue.
Le producer Reddit original reste disponible dans le dossier `/archives`.
