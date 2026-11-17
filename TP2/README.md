# TP2 — Accidents routiers avec PySpark

**Auteur :** Alex Abdallah · ID16

**Notebook :** [Road_Crash_PySpark.ipynb](Road_Crash_PySpark.ipynb)

## Objectifs et données

L’étude porte sur **127 672 accidents** et **272 591 unités impliquées** en Australie-Méridionale
de 2012 à 2019. Elle vérifie deux requêtes de l’analyse de Rakesh Nain avec trois API Spark, puis
prolonge l’étude de gravité par une classification en Spark ML avec un test chronologique.

| Fichier ou dossier | Contenu |
|:---|:---|
| [`Road_Crash_PySpark.ipynb`](Road_Crash_PySpark.ipynb) | Analyses, résultats et modèle de gravité. |
| [`data/crash_data/`](data/crash_data/) | Huit CSV annuels. Une ligne représente un accident. |
| [`data/units_data/`](data/units_data/) | Huit CSV annuels. Un accident peut impliquer plusieurs unités. |
| [`data/empreintes.json`](data/empreintes.json) | Empreintes SHA-256 des seize CSV, contrôlées à la lecture. |
| [Dictionnaire des champs](data/metadata-for-road-crash.pdf) | Définitions des colonnes et des niveaux de gravité. |

## Parcours du notebook

| Étape | Travail réalisé |
|:---|:---|
| `[Data]` | Seize CSV contrôlés par SHA-256 et lecture avec RDD et DataFrame. |
| `[EDA]` | Partitionnement des immatriculations, âge, année de véhicule et gravité selon les signalements. |
| `[Prétraitement]` | Contrôle des clés, types numériques, cohérence du nombre de victimes et préparation de la cible. |
| `[Transformation]` | Deux requêtes comparées en RDD, DataFrame et SQL, mesures de durée, analyses par année, type et heure. |
| `[ML]` | Régression logistique ajustée sur 2012–2017 et seuil fixé sur 2018. |
| `[Éval]` | Matrice de confusion et aire sous la courbe précision-rappel sur les accidents de 2019. |
| `[Déploiement]` | Export et rechargement du `PipelineModel` pour prédire sur un CSV sans étiquette. |
| `[Conclusion]` | Synthèse du comptage, des performances et du périmètre du modèle. |

## Corrections des requêtes

| Problème | Changement apporté |
|:---|:---|
| Champs CSV séparés au moyen de `split(',')` et lus par position | Utiliser le parseur CSV et les noms de colonnes, puis vérifier les effectifs avec les deux lectures. |
| Total accident répété par la jointure avec les unités | Sélectionner les accidents qui impliquent au moins une unité sans permis. La jointure directe totalisait 2 072 victimes, le comptage par accident en donne 2 053. |
| Année, mois et jour de semaine réunis sous le nom « date » | Garder les champs distincts dans les trois projections. |
| Résultats bruts chargés en Python pour certains exemples | Limiter les aperçus et agréger les résultats dans Spark. |

## Durées et résultats

La requête sur les unités sans permis produit les mêmes lignes dans les trois API. Après un
échauffement par API, la médiane des trois essais donne 0,340 s en RDD, 0,040 s en DataFrame
et 0,042 s en Spark SQL. DataFrame et SQL partagent l’optimiseur de Spark. Le RDD exécute une
partie du travail dans Python. Ces chronos portent sur cette requête en mode local.

Le test sur 2019 contient 839 accidents graves ou mortels parmi 12 964 accidents enregistrés.
L’aire sous la courbe précision-rappel (AUC PR) est de **0,1352**, pour une prévalence de **6,47 %**.
Au seuil décidé sur les données de 2018, le modèle retrouve **253 cas graves** et déclenche
**1 151 fausses alertes**. La cible est la gravité parmi les accidents enregistrés.

## Exécution et sorties

Installer l’environnement commun, puis lancer toutes les cellules depuis la racine ou `TP2/`.
Les exports sont écrits dans `resultats_road_crash/`, dont `modele_gravite_spark/` pour le pipeline
et `predictions_gravite.csv` pour la démonstration par lot. [Installation et réexécution](../README.md#5-réexécuter-les-notebooks)

## Sources

- [Analyse des accidents avec PySpark par Rakesh Nain](https://medium.com/rakesh-nain/big-data-analysis-of-road-crash-data-using-pyspark-with-pyspark-tutorial-c78ff2c35588)
- [Instantané du code et des données](https://github.com/RakeshNain/Big-data-Analysis-of-Road-Crash-Data-using-PySpark-with-PySpark-Tutorial/tree/dab8e6466747903bf1e5bd21ce63fea31bfb6dcc)
- [Portail de données de l’Australie-Méridionale](https://data.sa.gov.au/)
