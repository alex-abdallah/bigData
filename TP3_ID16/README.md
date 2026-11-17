# TP3 — Deux cas personnels en Small Data et Big Data

**Alex Abdallah · ID16** · Polytech Marseille, FISE 5A Informatique

Les deux études reprennent un notebook Python publié sur Kaggle. Le cas Small traite la fraude
à partir d’un tableau de demandes d’ouverture de compte. Le cas Big estime la polarité de textes
avec Spark. Chaque notebook relie le code de départ aux corrections, puis suit les données jusqu’au
rechargement du modèle.

## 1. Fichiers du rendu

| Cas | Notebook exécuté | Données | Modèle et exemples |
|:---|:---|:---|:---|
| Small | [Fraude bancaire](Small/TP3_small_fraude_bancaire.ipynb) | [Un million de demandes BAF](Small/bank_fraud_dataset/) | [Artefacts Small](Small/artefacts/) |
| Big | [Sentiment des avis Amazon](Big/TP3_big_sentiment_amazon_spark.ipynb) | [Quatre millions d’avis Amazon Polarity](Big/amazon_reviews_dataset/) | [Artefacts Big](Big/artefacts/) |

```text
TP3_ID16/
├── README.md
├── Small/
│   ├── TP3_small_fraude_bancaire.ipynb
│   ├── bank_fraud_dataset/     Base.csv.gz et sa provenance
│   ├── artefacts/              Modèle Joblib, seuil, métriques, graphiques et exemples
│   └── requirements.txt
└── Big/
    ├── TP3_big_sentiment_amazon_spark.ipynb
    ├── amazon_reviews_dataset/  Onze fichiers d’apprentissage et un fichier de test
    ├── artefacts/              PipelineModel Spark, métriques, graphiques et exemples
    └── requirements.txt
```

## 2. Reprise du code et ajouts PySpark

| Cas | Bloc extrait du code de départ | Traitement dans le TP |
|:---|:---|:---|
| Small | Comptage de `fraud_bool` et valeurs manquantes par classe dans les sections 1.1 et 1.2 | Comptage du corpus complet et profil des absences par classe sur l’apprentissage. Comparaison pandas, DataFrame et Spark SQL. |
| Small | `test_classifier` choisit le seuil à partir de `y_test` dans la section 5.4 | Encodage appris sur les mois 0 à 5. Le mois 6 fixe le seuil sous un budget de fausses alertes, puis le mois 7 sert au test. |
| Big | `get_labels_and_texts` lit les textes bzip2 dans des listes Python et convertit l’étiquette en 0 ou 1 | Lecture Spark des CSV gzip et conversion du champ `class_index`. Détection des répétitions et des textes communs à l’apprentissage et au test. |
| Big | `normalize_texts` passe en minuscules et filtre les caractères | Reproduction du filtre sur deux phrases de contrôle. Le tokenizer Spark conserve les chiffres, les lettres accentuées et les apostrophes. |
| Big | Keras prépare les séquences et entraîne deux réseaux de neurones | Spark ML calcule le TF-IDF et entraîne une régression logistique et un Naïve Bayes. |

Les notebooks de départ, tous deux en version 3, utilisent pandas ou des listes Python, scikit-learn
et Keras selon l’étude. Le rapprochement ci-dessus désigne les opérations reprises et le code qui les
fait évoluer. [Matthew McNulty](https://www.kaggle.com/code/matthewmcnulty/bank-account-fraud) · [MuonNeutrino](https://www.kaggle.com/code/muonneutrino/sentiment-analysis-with-amazon-reviews)

## 3. Étapes des deux notebooks

La première cellule donne le titre et **Ma contribution**, avec le bilan d’exécution, les problèmes,
les modifications et les conditions d’utilisation. Chaque phase commence par une cellule `## [Étape]`.

| Étape | Small · Fraude bancaire | Big · Avis Amazon |
|:---|:---|:---|
| `[Data]` | Charger le fichier Base et reprendre le comptage de la cible. | Lire les douze fichiers avec le schéma et le codage de la polarité. |
| `[EDA]` | Étudier la prévalence mensuelle, les absences et les catégories de paiement sur l’apprentissage. | Décrire la polarité, les titres vides et les longueurs de texte. |
| `[Prétraitement]` | Dédoublonner, appliquer la fiche Feedzai et comparer pandas, Spark DataFrame et Spark SQL. | Normaliser les textes et retirer les répétitions, les contradictions et les textes déjà vus. |
| `[Transformation]` | Ajuster l’encodeur des catégories sur l’apprentissage. | Tester les règles de tokenisation et ajuster les poids TF-IDF. |
| `[ML]` | Comparer les boostings standard et pondéré, puis fixer le seuil sur le mois 6. | Entraîner la régression logistique et le Naïve Bayes dans Spark ML. |
| `[Éval]` | Mesurer le classement, le volume d’alertes et la calibration au mois 7. | Mesurer le F1 macro, la précision, le rappel et les erreurs selon la longueur. |
| `[Benchmark]` |  | Répéter le même agrégat Spark sur les CSV gzip et leur copie temporaire en Parquet. |
| `[Déploiement]` | Recharger le modèle et son seuil, puis prédire sur cinq demandes. | Recharger le pipeline TF-IDF et exporter les prédictions de trois avis. |
| `[Conclusion]` | Relier le seuil au nombre de dossiers à contrôler. | Résumer les performances, la préparation et le coût des lectures. |

## Small — Fraude bancaire

### Données et protocole

Le fichier Base contient **988 971 demandes légitimes** et **11 029 fraudes**, soit **1,1029 %**.
La fiche Feedzai signale une absence avec `-1` dans les champs de durée et de décompte concernés.
Dans `intended_balcon_amount`, chaque valeur négative représente une absence. Cela concerne
**742 523 lignes** du fichier. Le nettoyage applique cette règle à l’apprentissage et à la
prédiction. Les contrôles Spark et pandas concordent sur les effectifs par mois et classe, les
absences par classe et les agrégats par catégorie de paiement. [Fiche Feedzai](https://raw.githubusercontent.com/feedzai/bank-account-fraud/main/documents/datasheet.pdf)

| Période | Demandes | Fraudes | Rôle |
|:---|---:|---:|:---|
| Mois 0 à 5 | 794 989 | 8 151 | Ajuster les transformations et les modèles. |
| Mois 6 | 108 168 | 1 450 | Choisir le modèle selon l’average precision et fixer son seuil sous un budget de 5 % de faux positifs. |
| Mois 7 | 96 843 | 1 428 | Appliquer le seuil fixé et mesurer les alertes et les fraudes détectées. |

### Résultats et seuil d’alerte

Sur le mois 6, le boosting pondéré atteint une average precision de **0,1784**, contre **0,1483**
pour le boosting standard. Le seuil retenu, **0,80955**, capte **52,55 %** des fraudes de validation
et signale **4,99 %** de ses demandes légitimes.

| Mesure sur le mois 7 | Valeur |
|:---|---:|
| Average precision du boosting pondéré | 0,2132 |
| Average precision d’un score constant | 0,0147 |
| Fraudes détectées sur 1 428 | 767 |
| Demandes légitimes signalées | 4 123 |
| Précision parmi les 4 890 alertes | 15,69 % |
| Rappel parmi les fraudes | 53,71 % |
| Taux de faux positifs | 4,32 % |

Chaque alerte représente un dossier à examiner. Le notebook détaille aussi la comparaison au
seuil de 0,50, les écarts par âge et la calibration des scores sur le mois de test.

## Big — Sentiment des avis Amazon

### Corpus et préparation

Spark lit **3 600 000 avis d’apprentissage** et **400 000 avis de test**. Il ramène à un seul avis
les répétitions dont les étiquettes s’accordent et écarte les groupes qui portent des polarités
contradictoires. Une jointure par empreinte retire du test les textes présents à l’apprentissage.

| Étape | Apprentissage | Test |
|:---|---:|---:|
| Avis lus | 3 600 000 | 400 000 |
| Texte ou étiquette invalide | 2 | 0 |
| Répétitions de même polarité | 337 | 4 |
| Étiquettes contradictoires | 2 | 0 |
| Textes déjà vus à l’apprentissage | 0 | 82 |
| Avis retenus | **3 599 659** | **399 914** |

### Mesures sur le test officiel nettoyé

| Modèle | Exactitude | F1 macro |
|:---|---:|---:|
| Classe majoritaire | 50,01 % | 0,3334 |
| Naïve Bayes | 82,85 % | 0,8284 |
| Régression logistique | **90,49 %** | **0,9049** |

Le F1 macro accorde le même poids aux deux polarités. Les graphiques montrent aussi le compromis
précision–rappel avec plusieurs seuils et les écarts selon la longueur des avis. Le corpus est
construit avec deux polarités et des effectifs équilibrés. [Corpus Amazon Polarity au format CSV](https://doi.org/10.34740/KAGGLE/DSV/5339021)

### Comparaison CSV gzip et Parquet

Le benchmark réutilise les **3,6 millions d’avis d’apprentissage**. L’agrégat compte les avis
et les caractères du corps par polarité. Chaque résultat est vérifié, avec un échauffement et
trois mesures par format.

| Format | Médiane | Minimum | Maximum | Stockage |
|:---|---:|---:|---:|---:|
| CSV gzip | 21,05 s | 19,08 s | 22,23 s | 618,2 Mo |
| Parquet Snappy | 4,59 s | 4,51 s | 4,93 s | 1 003,4 Mo |

L’écriture Parquet a pris **24,60 s**. Dans cet essai local, la durée médiane de l’agrégation
est divisée par **4,6** avec Parquet. Les CSV gzip occupent moins d’espace sur disque.
[Six mesures de l’exécution complète](Big/artefacts/benchmark_formats.csv)

## Exécution et sorties

### Réexécuter les deux notebooks

L’exécution complète a été vérifiée avec Python 3.12, Java 21 et Spark 4.0.1. Depuis la racine,
installer les dépendances dans le noyau Python choisi. Chaque notebook peut partir de la racine,
de `TP3_ID16/` ou de son dossier. Exécuter les deux études l’une après l’autre.
[Installation commune](../README.md#5-réexécuter-les-notebooks)

```bash
python -m pip install -r TP3_ID16/Small/requirements.txt -r TP3_ID16/Big/requirements.txt
```

| Dossier généré | Résultats |
|:---|:---|
| `Small/sorties/` | Modèle Joblib, seuil, métriques, graphiques, exemples et prédictions. |
| `Big/sorties/complet/` | Pipeline Spark, métriques, benchmark des formats, exemples et prédictions. |
| `Big/sorties/rapide/` | Mêmes sorties pour la fraction de données choisie. |

Le mode complet Big utilise par défaut `local[4]`, 6 Gio de mémoire JVM et 32 partitions de
shuffle. Le benchmark génère un Parquet temporaire. Prévoir plusieurs gigaoctets libres pour
les données intermédiaires Spark. `TP3_SPARK_MASTER`, `TP3_SPARK_MEMORY` et `TP3_SPARK_SHUFFLE`
permettent d’ajuster les réglages. Avec `BIGDATA_OUTPUT_ROOT`, les sorties vont sous `TP3_Small/`
et `TP3_Big/` dans le répertoire choisi.

### Essai Big sur 2 % des avis

Définir `TP3_BIG_FRACTION=0.02` avant le démarrage du noyau. Spark lit les fichiers puis échantillonne
les avis pour les étapes suivantes. Les volumes, les mesures et le modèle exporté reflètent ce sous-ensemble.

### Réutiliser les modèles fournis

La phase Déploiement illustre l’inférence à partir du CSV d’exemple. Pour employer le modèle livré,
partir des imports et des fonctions de la première cellule de code de l’étude, puis pointer sur `artefacts/`.

- **Small.** Charger `modele_fraude.joblib` avec `joblib.load`, lire `exemple_entree.csv` avec
  `pd.read_csv` et appeler `predict`. Cette fonction applique la préparation des montants, puis le
  seuil fourni avec le modèle. [Modèle et seuil Small](Small/artefacts/)
- **Big.** Démarrer Spark avec `create_spark`, charger `modele_sentiment_spark` avec
  `PipelineModel.load`, lire `exemple_entree.csv` avec les options de la section Déploiement et appeler
  `score_reviews`. Le pipeline contient la normalisation, le TF-IDF et le classifieur. Pour relire les
  exports dans `predictions/`, utiliser `spark.read.options(header=True, multiLine=True).csv(...)` avec
  l’échappement par défaut de Spark. [Pipeline Big](Big/artefacts/)
