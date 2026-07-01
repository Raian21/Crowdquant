# ⚙️ CrowdQuant - Module Processing (Couche Silver)

Ce dossier contient les scripts de transformation de données (ETL/ELT) du projet CrowdQuant. Il gère le passage des données brutes (Couche Bronze) vers des données nettoyées et dédupliquées (Couche Silver) au sein de notre architecture Medallion sur Azure.

## 🏛️ Architecture et Objectifs

L'objectif de ce module est de préparer les données textuelles (Social/News) et financières (Prices) pour qu'elles puissent être consommées par les modèles de Machine Learning (FinBERT) et les analyses agrégées de la couche Gold.

Flux de traitement actuel :
1. Extraction : Lecture des données brutes depuis le conteneur Azure bronze-crowdquant (fichiers JSONL).
2. Transformation (PySpark) :
   - Normalisation des schémas.
   - Suppression massive des doublons générés par les API d'ingestion (NewsAPI, GDELT, Reddit).
   - Nettoyage des textes et formatage des dates (event_time).
3. Chargement : Écriture des données propres dans le conteneur silver-crowdquant sous forme de fichiers JSONL partitionnés par date.

## 🛠️ Mécanismes Clés Développés

* Traitement PySpark par Micro-lots (Micro-batching) : Pour éviter les erreurs de type OutOfMemory, les données sont lues et traitées par petits lots (ex: 50 blobs à la fois).
* Gestion d'État (Incremental Loading) : Un fichier silver_state.json est maintenu dans la couche Silver. Il agit comme un "marque-page" permettant au job de ne traiter que les *nouveaux* fichiers Bronze lors de sa prochaine exécution, ignorant l'historique déjà traité.
* Déduplication Massive : La couche Bronze agissant en "Append-Only" (ajout continu), elle contient d'énormes volumes de doublons (comportement normal des API). Le processing Silver applique un .dropDuplicates() strict pour garantir l'unicité de chaque article avant l'analyse NLP.

## 👨‍💻 Instructions pour Mohamed (Git & Intégration)

Mohamed, ta mission ici est de versionner ce code PySpark sur notre dépôt GitHub et de préparer l'environnement pour tes propres intégrations Talend (Cold Start).

1. Récupération et création de la branche :
Assure-toi d'être à jour sur le dépôt local, puis crée une nouvelle branche dédiée au processing :
```bash
git checkout main
git pull origin main
git checkout -b feature/processing-silver