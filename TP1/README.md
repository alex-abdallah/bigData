# TP — Comptage de mots avec Hadoop MapReduce et PySpark

**Auteur :** Alex Abdallah  
**Cours :** Fondements et Applications de Big Data

## Objectif

Implémenter et comparer cinq méthodes de comptage des occurrences
de mots dans un fichier :

1. Python séquentiel simple.
2. Pseudo-MapReduce en Python.
3. Hadoop MapReduce.
4. PySpark RDD avec `flatMap` et `reduceByKey`.
5. PySpark DataFrame avec `explode` et `groupBy().count()`.

## Fichiers de test

Le fichier initial contient 1 000 lignes, avec un mot par ligne
et des répétitions.

Ce fichier est dupliqué m fois pour produire les tailles suivantes :

| Facteur m | Nombre de lignes |
|---:|---:|
| 1 | 1 000 |
| 10 | 10 000 |
| 100 | 100 000 |
| 1 000 | 1 000 000 |

Les mêmes fichiers sont utilisés pour les cinq méthodes.
Les quatre fichiers d’origine sont inclus ; le notebook peut aussi les régénérer à partir de la même graine.

## Contenu du dépôt

- `TP_ComptageMots.ipynb` : explications, code, tests, comparaison et conclusions.
- `compose.yaml` : configuration des services Hadoop.
- `config` : paramètres HDFS, YARN et MapReduce.
- `wordcount_spark.py` : programme de comptage avec PySpark.

## Exécution

Installer les dépendances du [projet](../README.md), ouvrir le [notebook](TP_ComptageMots.ipynb) et sélectionner **Run All**.

- Les deux méthodes Python et les deux méthodes PySpark recalculent les comptes sur les quatre volumes. Les durées affichées sont les médianes de trois essais. Les deux versions PySpark tournent dans la même session à deux threads, après échauffement.
- Hadoop relit les comptages produits par les jobs de l’expérience originale et compare chaque compte au résultat Python. Le temps affiché est la médiane des trois mesures archivées dans [temps_hadoop.csv](resultats/temps_hadoop.csv).

### Refaire le calcul Hadoop avec Docker

Depuis `TP1/`, avec Docker et Compose démarrés :

```bash
docker compose pull
docker compose up -d
docker compose exec -T namenode hdfs dfsadmin -report
docker compose exec -T resourcemanager yarn node -list
export TP1_HADOOP_LIVE=1
```

Quand HDFS voit un DataNode actif et YARN un nœud `RUNNING`, lancer le notebook depuis un environnement qui hérite de cette variable. La cellule Hadoop copie elle-même les fichiers dans HDFS et lance un job par volume. Une fois l’expérience terminée, utiliser `docker compose stop`.

Le dossier [resultats](resultats/) conserve les comptes, les temps et les journaux de l’expérience Hadoop. Le [programme PySpark original](wordcount_spark.py) est aussi fourni.

## Interpréter les temps

Les mesures ne couvrent pas exactement les mêmes étapes : Hadoop inclut la soumission du job, alors que PySpark exclut la création de la session Spark. L’expérience porte sur une seule machine et au plus un million de lignes.
