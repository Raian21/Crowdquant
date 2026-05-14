📘 Guide de Connexion au Data Lake - Projet CrowdQuant
Salut l'équipe !

L'infrastructure Cloud de notre projet est désormais en place. Toute la collecte de données en temps réel est opérationnelle et les fichiers sont centralisés sur un Data Lake Gen2 Microsoft Azure.

Voici les instructions pour que vous puissiez accéder à notre "or noir" et commencer l'entraînement de vos modèles (Machine Learning, FinBERT, etc.) sans perturber le pipeline d'ingestion.

Il existe deux façons de vous connecter, selon vos besoins :

L'accès visuel (Portail Azure) : Pour explorer les dossiers.

L'accès code (Jeton SAS) : Pour charger les données directement dans vos Notebooks Python.

1️⃣ L'accès Visuel : Naviguer dans les dossiers via le Portail
Je vous ai ajoutés au projet via vos adresses e-mails étudiantes avec le rôle de Lecteurs des données Blob du stockage. Cela vous permet de visualiser tout le contenu, sans risquer de supprimer ou modifier des données par erreur.

Comment y accéder :

Connectez-vous sur portal.azure.com avec votre compte Microsoft Azure étudiant.

Dans la barre de recherche en haut, tapez Comptes de stockage (Storage accounts) et cliquez sur notre compte de projet.

Dans le menu latéral de gauche, descendez jusqu'à la section Stockage des données et cliquez sur Conteneurs.

Vous verrez apparaître notre architecture en 3 couches :

🥉 cq-bronze : Les données brutes. N'y touchez pas, c'est la sauvegarde de sécurité.

🥈 cq-silver : Les données nettoyées et filtrées. C'est ici que vous allez récupérer la donnée pour vos modèles.

🥇 cq-gold : Les tables finales prêtes pour l'application Streamlit.

2️⃣ L'accès Code : Connecter vos scripts avec le SAS Token
Pour que vos scripts (Pandas ou PySpark) puissent lire les données dans le Cloud sans mot de passe complexe, nous utilisons une clé de sécurité appelée Jeton SAS (Shared Access Signature). Je vais vous envoyer ce jeton (une longue URL) sur notre canal de messagerie.

⚠️ Règle d'or de sécurité (TRÈS IMPORTANT)
Ne collez JAMAIS cette URL SAS directement dans vos fichiers de code .ipynb ou .py ! Si notre code est publié sur GitHub, n'importe qui sur internet aura accès à notre Cloud et videra nos crédits.

La bonne méthode (Le fichier .env) :
Étape A : Configuration locale sur votre PC

À la racine de notre projet sur votre machine, créez un fichier nommé exactement .env (n'oubliez pas le point devant).

Ouvrez-le et collez-y la variable suivante en y ajoutant le lien que je vous ai fourni :

Plaintext
AZURE_SAS_TOKEN_SILVER="https://nomducompte.blob.core.windows.net/cq-silver?sp=rl&st=2024-05..."
Sauvegardez le fichier. (Ne vous inquiétez pas pour GitHub, notre fichier .gitignore est configuré pour bloquer les fichiers .env).

Étape B : Charger les données dans Python