# TP3 — Deux cas personnels : Small Data et Big Data

**Auteur :** Alex Abdallah · ID16

## Les deux cas et leurs fichiers

| | Small | Big |
|:---|:---|:---|
| Notebook | [Fraude bancaire](Small/TP3_small_fraude_bancaire.ipynb) | [Sentiment des avis Amazon](Big/TP3_big_sentiment_amazon_spark.ipynb) |
| Corpus | [BAF : un million de demandes](Small/bank_fraud_dataset/) | [Amazon Polarity : quatre millions d’avis](Big/amazon_reviews_dataset/) |
| Traitement | PySpark et pandas, modèles scikit-learn | PySpark et Spark ML |
| Évaluation | Validation au mois 6, test au mois 7 | Test officiel dédoublonné |
| Prédiction | Modèle et seuil sauvegardés dans un fichier Joblib | Chaîne TF-IDF et régression logistique sauvegardées dans un `PipelineModel` |

## Étapes des deux notebooks

| Étape | Small · Fraude bancaire | Big · Avis Amazon |
|:---|:---|:---|
| `[Data]` | Lire `Base.csv.gz` et identifier la cible `fraud_bool` et l’indice `month`. | Lire onze fichiers gzip d’apprentissage et le fichier de test selon un schéma Spark explicite. |
| `[EDA]` | Mesurer la rareté de la fraude, les valeurs indisponibles et les différences entre modalités. | Comparer les polarités, les champs vides et les longueurs de texte. |
| `[Prétraitement]` | Convertir les sentinelles et vérifier les agrégats Spark et pandas sur le CSV complet. | Normaliser les avis, traiter les répétitions et exclure du test les textes vus à l’apprentissage. |
| `[Transformation]` | Encoder les catégories avec un pipeline scikit-learn. | Tokeniser les textes, calculer les fréquences et ajuster les poids IDF sur l’apprentissage. |
| `[ML]` | Comparer deux variantes de boosting sur le mois 6 et fixer le seuil d’alerte. | Entraîner une régression logistique et un Naïve Bayes dans Spark ML. |
| `[Éval]` | Tester sur le mois 7 et mesurer le rappel, les fausses alertes et la calibration. | Comparer les modèles au repère majoritaire et analyser les erreurs du test officiel. |
| `[Déploiement]` | Recharger le pipeline et son seuil pour scorer un CSV de demandes. | Recharger la chaîne Spark puis exporter les prédictions d’un lot d’avis. |
| `[Conclusion]` | Relier la qualité du classement au volume de dossiers à contrôler. | Interpréter les métriques selon le corpus et les limites de la représentation TF-IDF. |

## Small — Fraude bancaire

### Question et protocole

Quels dossiers contrôler en priorité quand la fraude est rare et que chaque alerte mobilise un
contrôle manuel ? Le jeu BAF de Feedzai contient un million de demandes synthétiques d’ouverture
de compte. Les mois 0 à 5 servent à l’apprentissage. Le mois 6 sert à choisir le modèle par
average precision puis à fixer le seuil sous un budget de 5 % de faux positifs. Le mois 7 fournit
le test chronologique avec ce même seuil. [Documentation du jeu BAF](https://github.com/feedzai/bank-account-fraud)

### Vérification du traitement PySpark

Spark et pandas retrouvent les mêmes effectifs mensuels et le même nombre de valeurs manquantes
après conversion des sentinelles. Le tableau du notebook détaille les colonnes concernées.

### Résultats du mois 7

Le boosting pondéré atteint une average precision de **0,2163** pour un repère de **0,0147**.
Parmi les 4 705 dossiers signalés, on retrouve **760 fraudes** et **3 945 demandes légitimes**.
Le rappel est de **53,22 %** sur les 1 428 fraudes. Le modèle pondéré classe mieux les dossiers
que le boosting standard sur ce mois, tandis que son score brut est moins bien calibré.

[Notebook de départ de Matthew McNulty](https://www.kaggle.com/code/matthewmcnulty/bank-account-fraud)
· [Publication BAF](https://arxiv.org/abs/2211.13358)

## Big — Sentiment des avis Amazon

### Corpus et préparation

Le titre et le texte servent à distinguer les avis positifs et négatifs. Spark lit **3,6 millions
d’avis d’apprentissage** et **400 000 avis de test**. Le nettoyage exclut les textes inutilisables,
les répétitions et les étiquettes contradictoires. Sur le test, 4 répétitions internes et 82 avis
dont le texte apparaît à l’apprentissage sont écartés. Les poids TF-IDF sont ajustés sur les avis
d’apprentissage retenus.

### Mesures sur les 399 914 avis de test retenus

| Modèle | Exactitude | F1 macro |
|:---|---:|---:|
| Classe majoritaire | 50,01 % | 0,3334 |
| Naïve Bayes | 82,85 % | 0,8284 |
| Régression logistique | **90,49 %** | **0,9049** |

Le F1 macro attribue le même poids aux deux polarités. Le notebook examine aussi le compromis
précision-rappel et les écarts selon la longueur du texte. Le corpus contient uniquement des avis
positifs ou négatifs, avec des effectifs construits pour être équilibrés.

[Notebook de départ de MuonNeutrino](https://www.kaggle.com/code/muonneutrino/sentiment-analysis-with-amazon-reviews)
· [Corpus publié sur Kaggle](https://doi.org/10.34740/KAGGLE/DSV/5339021)

## Exécution et sorties

Exécuter un notebook à la fois depuis la racine, `TP3_ID16/` ou le dossier de l’étude. Les corpus
compressés sont fournis dans les dossiers indiqués en haut de cette page. [Installation et réexécution](../README.md#5-réexécuter-les-notebooks)

| Dossier de sortie | Fichiers obtenus |
|:---|:---|
| `Small/sorties/` | Modèle Joblib et seuil, exemples d’entrée, prédictions, graphiques et métriques. |
| `Big/sorties/complet/` | `PipelineModel` Spark, métriques du test complet et exemple prédit par lot. |
| `Big/sorties/rapide/` | Même structure pour un essai sur une fraction des avis. |

Les dossiers `artefacts/` contiennent les sorties de la première exécution livrée.

### Essai Big sur 2 % des avis

Définir `TP3_BIG_FRACTION=0.02` avant de lancer le noyau. Spark parcourt les fichiers source et
tire un échantillon pour l’entraînement et l’évaluation. Le mode complet utilise par défaut
`local[4]`, `6g` de mémoire JVM et 32 partitions de shuffle. Les variables `TP3_SPARK_MASTER`,
`TP3_SPARK_MEMORY` et `TP3_SPARK_SHUFFLE` permettent d’ajuster ces réglages.
