# TP1 — Comptage de mots

**Auteur :** Alex Abdallah · ID16 · **Date :** 17 novembre 2026

**Notebook :** [TP_ComptageMots.ipynb](TP_ComptageMots.ipynb)

## Objectif et fichiers

Le même corpus est traité par quatre méthodes. Le notebook vérifie chaque compte avant de comparer les durées.

| Fichier ou dossier | Rôle |
|:---|:---|
| [`TP_ComptageMots.ipynb`](TP_ComptageMots.ipynb) | Préparation du corpus, quatre calculs, tableau et graphique de comparaison. |
| [`WordCountHadoop.java`](WordCountHadoop.java) | Mapper, combiner, reducer et configuration du job Hadoop. |
| [`wordcount_spark.py`](wordcount_spark.py) | Exécution autonome du calcul RDD sur les quatre fichiers. |
| [`data/`](data/) | Fichiers textes de taille croissante avec un mot par ligne. |
| [`resultats/`](resultats/) | Comptes, chronos et journaux de l’expérience HDFS/YARN. |

## Plan du notebook

| Méthode | Principe |
|:---|:---|
| Python séquentiel | Lecture par ligne et mise à jour d’un dictionnaire. |
| PySpark | Extraction des mots en RDD et agrégation par `reduceByKey`. La variante DataFrame fournit une vérification supplémentaire. |
| Python pseudo-MapReduce | Émission, regroupement et réduction des paires dans un processus Python. |
| Hadoop MapReduce | Job Java avec mapper, combiner et reducer exécuté par le moteur Hadoop `LocalJobRunner`. |

## Protocole et mesures

Le fichier de 1 000 lignes contient dix mots distincts. Les trois fichiers suivants répètent ce
bloc 10, 100 et 1 000 fois. Le notebook vérifie le dictionnaire complet à chaque essai, soit 48
vérifications. Le tableau donne les médianes de trois essais en secondes. La création de la
session Spark et la compilation Java sont hors mesure. Chaque mesure Hadoop inclut le lancement
de Java et l’écriture du résultat.

| Lignes | Python séquentiel | PySpark RDD | Python pseudo-MapReduce | Hadoop MapReduce |
|---:|---:|---:|---:|---:|
| 1 000 | 0,0051 | 0,5347 | 0,0099 | 7,4931 |
| 10 000 | 0,0114 | 0,4188 | 0,0206 | 7,4770 |
| 100 000 | 0,0693 | 0,5516 | 0,1183 | 7,4536 |
| 1 000 000 | 0,7899 | 1,4635 | 1,1846 | 7,7357 |

Python séquentiel offre les durées les plus courtes sur les quatre fichiers. Son dictionnaire tient
dix compteurs, tandis que le pseudo-MapReduce conserve les occurrences intermédiaires avant la
réduction. La durée Hadoop varie peu à cette échelle. Le lancement de Java et du job fait partie
du coût mesuré. Avec Spark, seules les dix paires finales remontent dans Python. Les durées dépendent
de la machine, du vocabulaire et de la configuration locale.

## Exécution

Installer l’environnement commun, puis lancer toutes les cellules depuis la racine ou `TP1/`.
Le JDK compile `WordCountHadoop.java` avec les bibliothèques Hadoop de PySpark. Le notebook utilise
un répertoire temporaire pour le job et sa sortie. Le script autonome se lance avec
`python TP1/wordcount_spark.py` depuis la racine. [Installation et réexécution](../README.md#5-réexécuter-les-notebooks)

## Expérience HDFS/YARN

Le dossier [`resultats/`](resultats/) conserve les comptes, mesures et journaux de l’expérience
HDFS/YARN initiale avec Hadoop 3.3.6. Ses durées sont distinctes des mesures du mode local ci-dessus.
Pour relancer un job de cette expérience, démarrer les services depuis `TP1/` et attendre un
DataNode actif et un nœud YARN `RUNNING`. [Configuration Docker Compose](compose.yaml)

```bash
docker compose up -d
docker compose exec -T namenode hdfs dfsadmin -report
docker compose exec -T resourcemanager yarn node -list

docker compose exec -T namenode hdfs dfs -mkdir -p /tp/input
docker compose exec -T namenode hdfs dfs -put -f /data/mots_m1.txt /tp/input/
sortie="/tp/resultat_m1_$(date +%s)"
docker compose exec -T namenode bash -lc \
  'hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount "$1" "$2"' \
  _ /tp/input/mots_m1.txt "$sortie"
docker compose exec -T namenode hdfs dfs -cat "$sortie/part-r-*"
docker compose stop
```

Remplacer `m1` par `m10`, `m100` ou `m1000` pour traiter les autres volumes. La
[version](resultats/version_hadoop.txt) et l’[empreinte de l’image Docker d’origine](resultats/image_hadoop.txt)
sont fournies avec les journaux.
