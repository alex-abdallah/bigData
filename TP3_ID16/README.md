# TP3 — Deux cas personnels : Small Data et Big Data

**Auteur :** Alex Abdallah · ID16

| | Small | Big |
|:---|:---|:---|
| Notebook | [Fraude bancaire](Small/TP3_small_fraude_bancaire.ipynb) | [Sentiment des avis Amazon](Big/TP3_big_sentiment_amazon_spark.ipynb) |
| Corpus | [BAF : un million de demandes](Small/bank_fraud_dataset/) | [Amazon Polarity : quatre millions d’avis](Big/amazon_reviews_dataset/) |
| Traitement | PySpark et pandas, modèles scikit-learn | PySpark et Spark ML |
| Évaluation | Validation au mois 6, test au mois 7 | Test officiel dédoublonné |
| Prédiction | Modèle et seuil sauvegardés dans un fichier Joblib | Chaîne TF-IDF et régression logistique sauvegardées dans un `PipelineModel` |

## Small — Fraude bancaire

Le [jeu BAF de Feedzai](https://github.com/feedzai/bank-account-fraud) contient des demandes
synthétiques d’ouverture de compte. Le prétraitement PySpark lit le corpus complet, dédoublonne
les demandes, nettoie les valeurs sentinelles et retrouve les mêmes agrégats que pandas.
Deux modèles de boosting sont ajustés sur les mois 0 à 5. Sur le mois 6, l’average precision
départage les modèles et le seuil maximise le rappel sous un budget de 5 % de faux positifs.
Le mois 7 mesure la performance future sans recalibrer le seuil. Le fichier Joblib est rechargé
pour scorer un CSV de cinq demandes.

**Résultats sur le mois 7.** Le boosting pondéré atteint une average precision de **0,2163**
(repère : **0,0147**). Au seuil fixé sur le mois 6, il retrouve **760 fraudes sur 1 428** et
signale **3 945** demandes légitimes.

[Notebook de départ de Matthew McNulty](https://www.kaggle.com/code/matthewmcnulty/bank-account-fraud)
· [Publication BAF](https://arxiv.org/abs/2211.13358)

## Big — Sentiment des avis Amazon

Spark lit 3,6 millions d’avis d’apprentissage et 400 000 avis de test. Le nettoyage exclut les
textes inutilisables, les répétitions, les labels contradictoires et les textes du test déjà vus
en apprentissage. La chaîne TF-IDF est ajustée uniquement sur l’apprentissage. Le test compare
régression logistique, Naïve Bayes et classe majoritaire. La démonstration recharge le modèle
logistique et reproduit les scores, puis génère des prédictions sur un CSV sans étiquette.

**Résultats sur 399 914 avis de test après nettoyage.** La régression logistique atteint une
exactitude de **90,49 %** et un F1 macro de **0,9049**, devant Naïve Bayes (**82,85 %** d’exactitude)
et la classe majoritaire (**50,01 %**).

[Notebook de départ de MuonNeutrino](https://www.kaggle.com/code/muonneutrino/sentiment-analysis-with-amazon-reviews)
· [Corpus publié sur Kaggle](https://doi.org/10.34740/KAGGLE/DSV/5339021)

## Rejouer les deux études

Suivre l’[installation commune](../README.md#réexécuter-les-notebooks), puis exécuter un notebook
à la fois depuis la racine, `TP3_ID16/` ou le dossier de l’étude. Chacun contient son CSV compressé,
ses résultats affichés, ses explications et sa conclusion. Les nouveaux exports vont dans
`Small/sorties/` ou `Big/sorties/`. Les dossiers `artefacts/` conservent les sorties livrées avec
le projet initial ; les notebooks se suffisent à eux-mêmes pour reproduire les résultats.

Pour un essai Amazon plus court, définir `TP3_BIG_FRACTION=0.02` avant de lancer le noyau.
Spark lit toujours les fichiers source mais n’entraîne et n’évalue que sur les lignes tirées.
Les exports complets vont dans `Big/sorties/complet/` et ceux de l’essai dans `Big/sorties/rapide/`.
Les réglages par défaut de l’étude Amazon sont `local[4]`, `6g` de mémoire JVM et 32 partitions
de shuffle ; ils se règlent via `TP3_SPARK_MASTER`, `TP3_SPARK_MEMORY` et `TP3_SPARK_SHUFFLE`.
