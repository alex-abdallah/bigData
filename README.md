# Big Data — Rendu des TP 1, 2 et 3

**Alex Abdallah · ID16** — Polytech Marseille, FISE 5A Informatique

Le TP1 compare quatre façons de compter les mots. Le TP2 analyse les accidents routiers avec
les RDD, les DataFrames et Spark SQL. Le TP3 applique les étapes d’un projet de données à deux
cas personnels, la fraude bancaire et le sentiment des avis Amazon.

## 1. Notebooks à ouvrir

| TP | Sujet | Dossier | Notebook | Documentation |
|:---|:---|:---|:---|:---|
| **TP1** | Comptage de mots avec Python séquentiel, PySpark, Python pseudo-MapReduce et Hadoop MapReduce | [TP1/](TP1/) | [Comptage de mots](TP1/TP_ComptageMots.ipynb) | [Protocole et mesures](TP1/README.md) |
| **TP2** | Accidents routiers en Australie-Méridionale avec PySpark | [TP2/](TP2/) | [Accidents routiers](TP2/Road_Crash_PySpark.ipynb) | [Requêtes et modèle](TP2/README.md) |
| **TP3 Small** | Détection de fraude bancaire avec pandas, PySpark et scikit-learn | [TP3_ID16/Small/](TP3_ID16/Small/) | [Fraude bancaire](TP3_ID16/Small/TP3_small_fraude_bancaire.ipynb) | [Étude Small](TP3_ID16/README.md#small--fraude-bancaire) |
| **TP3 Big** | Classification du sentiment de quatre millions d’avis Amazon avec PySpark | [TP3_ID16/Big/](TP3_ID16/Big/) | [Avis Amazon](TP3_ID16/Big/TP3_big_sentiment_amazon_spark.ipynb) | [Étude Big](TP3_ID16/README.md#big--sentiment-des-avis-amazon) |

Les tableaux, graphiques et mesures sont enregistrés dans les notebooks et se consultent directement sur GitHub.

## 2. Arborescence du rendu

```text
bigData/
├── README.md                            Index et procédure de réexécution
├── requirements.txt                     Dépendances communes
├── TP1/
│   ├── README.md                         Méthodes et mesures
│   ├── TP_ComptageMots.ipynb               Comptage et comparaison
│   ├── wordcount.py                      Comptage Python séquentiel
│   ├── mapreduce_wordcount.py            Phases map, shuffle et reduce en Python
│   ├── hadoop_wordcount.py               Lancement du WordCount officiel et lecture des compteurs
│   ├── wordcount_spark.py                 Comptage PySpark en RDD et DataFrame
│   ├── requirements.txt                  Dépendances du TP1
│   ├── compose.yaml                      Services de l’expérience HDFS/YARN
│   └── data/                             Quatre fichiers fournis, cinquième généré à 10 millions de lignes
├── TP2/
│   ├── README.md                         Analyses et résultats
│   ├── Road_Crash_PySpark.ipynb             Étude et modèle de gravité
│   └── data/                             Accidents, unités, empreintes et dictionnaire des champs
└── TP3_ID16/
    ├── README.md                         Démarche des deux études
    ├── Small/
    │   ├── TP3_small_fraude_bancaire.ipynb
    │   └── bank_fraud_dataset/             Un million de demandes synthétiques
    └── Big/
        ├── TP3_big_sentiment_amazon_spark.ipynb
        └── amazon_reviews_dataset/        Corpus d’apprentissage et de test compressé
```

Le `PipelineModel` du TP2 et les exports des deux cas du TP3 sont produits à l’exécution. Les fichiers utiles à chaque expérience sont indiqués dans le README du dossier correspondant.

## 3. Vérification des consignes de remise

| Élément demandé | Où le vérifier |
|:---|:---|
| Emplacements `TP1/`, `TP2/` et `TP3_ID16/{Small,Big}/` | Arborescence ci-dessus. `16` est l’identifiant de l’auteur. |
| Titre, date et identité pour le TP1 | Première cellule Markdown du notebook. |
| Titre et contribution pour les TP2 et TP3 | Première cellule Markdown des trois notebooks, avec le bilan d’exécution, les problèmes rencontrés et les modifications. |
| Étiquettes des étapes du processus IA | Une cellule de titre au début de chaque phase dans les notebooks des TP2 et TP3. |
| Traitement PySpark pour le TP3 | Dans Small, la préparation du million de lignes est reproduite avec Spark et comparée à pandas. Dans Big, le pipeline utilise Spark de la lecture à la prédiction. |
| Explications, commentaires et conclusions | Le texte introduit les traitements, interprète les sorties et conclut chaque étude. |
| README à la racine | Ce document relie les fichiers de remise et les guides de chaque TP. |

Les phases portent les titres `## [Data]`, `## [EDA]`, `## [Prétraitement]`,
`## [Transformation]`, `## [ML]`, `## [Éval]` et `## [Déploiement]`. Une cellule
`## [Conclusion]` ferme chacun des trois notebooks balisés.

## 4. Résultats en un coup d’œil

| TP | Résultat observé |
|:---|:---|
| **TP1** | Les quatre méthodes donnent les mêmes occurrences sur cinq volumes, de 1 000 à 10 millions de lignes. PySpark DataFrame atteint une médiane de 1,804 s sur le plus grand fichier dans la série publiée. Le notebook compare aussi les deux modes Hadoop, les cœurs et le cache Spark. |
| **TP2** | Les trois API concordent sur deux requêtes. En comptant chaque accident une seule fois, la requête sur les unités sans permis donne 2 053 victimes. En 2019, le modèle détecte 253 des 839 accidents graves ou mortels au seuil fixé sur 2018. |
| **TP3 Small** | PySpark et pandas retrouvent les mêmes effectifs et valeurs manquantes. Sur le dernier mois, le boosting pondéré retrouve 760 fraudes sur 1 428, pour 3 945 fausses alertes et une average precision de 0,2163. |
| **TP3 Big** | Après dédoublonnage, la régression logistique atteint 90,49 % d’exactitude et 0,9049 de F1 macro sur 399 914 avis du test officiel. |

## 5. Réexécuter les notebooks

**Prérequis.** Python 3.11 ou 3.12 et Java 17 ou 21. Le fichier
`requirements.txt` installe PySpark 4.0.1 et les bibliothèques Python des quatre notebooks.
Les corpus sont fournis dans le dépôt, qui représente près de 900 Mo de données. [Documentation Spark 4.0.1](https://spark.apache.org/docs/4.0.1/)

| Système | Où lancer les commandes |
|:---|:---|
| macOS ou Linux | Dans le terminal du système, à la racine du dépôt. |
| Windows | Dans le terminal Ubuntu via WSL, à la racine du dépôt. |

L’exécution intégrale sur les données complètes a été vérifiée dans Ubuntu via WSL avec Python 3.12,
Java 21 et Spark 4.0.1. Le TP1 lance le WordCount d’Apache Hadoop en mode local par défaut.
Le même programme a aussi été vérifié avec HDFS et YARN. [Modes Hadoop du TP1](TP1/README.md#3-installation-et-réexécution)
Sur Windows avec VS Code, ouvrir le dossier dans la session WSL pour sélectionner le noyau de cette session.

```bash
python3 --version
java -version
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m ipykernel install --sys-prefix --name python3
```

1. Ouvrir le notebook indiqué dans le tableau avec le noyau Python de `.venv` dans VS Code ou Jupyter.
2. Lancer toutes les cellules de haut en bas. Chaque TP peut démarrer indépendamment.
3. Pour le TP3 Big, prévoir environ 12 Go de mémoire disponible et plusieurs gigaoctets libres pour Spark en mode complet. Un essai sur 2 % des avis est documenté dans le guide du TP3. [Options du TP3](TP3_ID16/README.md#exécution-et-sorties)

<details>
<summary>Exécuter les quatre notebooks en ligne de commande</summary>

```bash
python -m nbconvert --to notebook --execute --output-dir=/tmp/bigdata-verification \
  --ExecutePreprocessor.timeout=3600 --ExecutePreprocessor.kernel_name=python3 \
  TP1/TP_ComptageMots.ipynb \
  TP2/Road_Crash_PySpark.ipynb \
  TP3_ID16/Small/TP3_small_fraude_bancaire.ipynb \
  TP3_ID16/Big/TP3_big_sentiment_amazon_spark.ipynb
```

Les copies exécutées vont dans `/tmp/bigdata-verification`. Les exports de données et de modèles
vont par défaut dans `TP2/resultats_road_crash/` et dans le dossier `sorties/` de chaque étude du TP3.
Définir `BIGDATA_OUTPUT_ROOT` permet de rediriger ces exports vers un répertoire de travail distinct.

</details>
