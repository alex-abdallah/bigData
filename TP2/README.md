# Analyse des accidents routiers avec PySpark

Étude de cas réalisée dans le cadre du cours de Big Data à Polytech Marseille.

**Auteur :** Alex Abdallah  
**Formation :** FISE 5A — Informatique

## Présentation

Ce projet analyse les données d’accidents routiers en Australie-Méridionale sur la période 2012–2019 à l’aide de PySpark.

Il s’appuie sur le tutoriel de Rakesh Nain, dont les cellules de code sont reproduites et accompagnées d’explications en français, de contrôles de qualité, de corrections et de visualisations complémentaires.

## Objectifs

- Manipuler des données avec les RDD, les DataFrames et Spark SQL.
- Comprendre le partitionnement, les transformations et les actions.
- Réaliser des filtrages, des jointures et des agrégations.
- Comparer les résultats obtenus avec les trois API.
- Identifier les limites des calculs et interpréter les résultats.

## Contenu du notebook

1. Préparation de l’environnement et vérification des données incluses.
2. Analyse avec les RDD.
3. Analyse avec les DataFrames.
4. Gravité et indicateurs alcool et stupéfiants.
5. Équivalence des requêtes RDD, DataFrame et SQL.
6. Évolution annuelle, jour de semaine et gravité par heure.
7. Plans d’exécution et chronométrage d’une jointure avec les trois API.
8. Conclusion et export des résultats.

L’étude porte sur **127 672 accidents** et **272 591 unités**. Les deux tables sont reliées par `REPORT_ID`. Le benchmark mesure trois fois la même jointure et agrégation par localité après échauffement, avec vérification des résultats à chaque essai.

## Technologies

- Python
- Apache Spark / PySpark
- pandas
- Matplotlib
- Jupyter ou VS Code

## Consulter le travail

Ouvrir le fichier [Road_Crash_PySpark.ipynb](Road_Crash_PySpark.ipynb) pour consulter le code, les explications et les sorties enregistrées.

## Exécution

Installer les dépendances du [projet](../README.md), puis lancer **Run All**. Pour Spark sous Windows, utiliser Ubuntu dans WSL. Les 16 CSV de 2012 à 2019 sont inclus dans [data/](data/), avec leurs empreintes SHA-256 et la documentation d’origine. Les exports CSV et Parquet sont écrits dans `resultats_road_crash/`, y compris `benchmark_api_spark.csv` et `gravite_par_heure.csv`.

L’ancien chargement par motif `*.csv` pouvait appeler la bibliothèque native Hadoop de Windows et échouer avec `NativeIO$Windows.access0`. Le notebook fournit désormais les chemins exacts à Spark et lit directement les données locales.

## Limites

Les résultats décrivent les accidents enregistrés dans le jeu de données. Sans données sur le trafic, ils ne permettent pas de mesurer directement un risque par trajet ou par kilomètre, ni d’établir des relations de causalité.

## Sources

- [Article de Rakesh Nain](https://medium.com/rakesh-nain/big-data-analysis-of-road-crash-data-using-pyspark-with-pyspark-tutorial-c78ff2c35588)
- [Dépôt original : code et données](https://github.com/RakeshNain/Big-data-Analysis-of-Road-Crash-Data-using-PySpark-with-PySpark-Tutorial)
- [Portail de données du gouvernement d’Australie-Méridionale](https://data.sa.gov.au/)
- [Documentation Apache Spark](https://spark.apache.org/docs/4.0.1/)

Le code original est attribué à Rakesh Nain. Les explications et analyses complémentaires sont présentées séparément dans le notebook.
