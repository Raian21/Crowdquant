# Projet CrowdQuant

# CrowdQuant — Module d’ingestion (Zone Bronze)

Ce module correspond à la couche d’ingestion de la plateforme **CrowdQuant**.  
Il est responsable de la collecte en temps réel de données **de marché** et **sociales** liées au Bitcoin, puis de leur injection dans **Apache Kafka** afin d’alimenter la couche **Bronze** du pipeline de données.

---

## Objectif

Mettre en place une ingestion continue, structurée et résiliente de données réelles afin de :

- alimenter les topics Kafka du projet ;
- constituer une couche Bronze exploitable ;
- préparer les futures couches **Silver** (normalisation) et **Gold** (indicateurs métier).

---

## Sources de données

Le module d’ingestion s’appuie actuellement sur plusieurs sources réelles.

### Sources marché
- **CoinGecko** → prix, volume, market cap, variation 24h
- **Yahoo Finance** → prix, open/high/low
- **CryptoCompare Price** → prix crypto

### Sources sociales / informationnelles
- **NewsAPI** → articles de presse généraliste
- **GDELT Project** → articles et métadonnées média
- **CryptoCompare News** → actualités spécialisées crypto

---

## Topics Kafka utilisés

- `prices_raw` → flux marché multi-sources
- `btc-social` → flux social / news lié au périmètre BTC

---

## Structure du dossier

```text
ingestion/
├── producers/
│   ├── producer_newsapi.py
│   ├── producer_gdelt.py
│   ├── producer_cryptocompare.py
│   ├── producer_prices_coingecko.py
│   ├── producer_prices_yf.py
│   └── ...
└── README.md
