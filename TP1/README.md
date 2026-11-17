# TP1 — Comptage de mots

**Auteur :** Alex Abdallah · ID16 · **Date :** 17 novembre 2026

**Notebook :** [TP_ComptageMots.ipynb](TP_ComptageMots.ipynb)

Le même corpus est traité par quatre méthodes. Le notebook vérifie chaque compte avant de comparer les temps.

| Méthode | Principe |
|:---|:---|
| Python séquentiel | Lecture par ligne et mise à jour d’un dictionnaire. |
| PySpark | Extraction des mots en RDD et agrégation par `reduceByKey`. La variante DataFrame fournit une vérification supplémentaire. |
| Python pseudo-MapReduce | Émission, regroupement et réduction des paires dans un processus Python. |
| Hadoop MapReduce | Job Java avec mapper, combiner et reducer exécuté par le moteur Hadoop `LocalJobRunner`. |

Les [fichiers fournis](data/) contiennent 1 000, 10 000, 100 000 et 1 000 000 de lignes, avec le
même vocabulaire de dix mots. Trois essais par méthode et par volume donnent 48 comptages vérifiés.
La comparaison affiche la médiane. Spark est initialisé hors mesure ; chaque essai Hadoop inclut
le lancement de Java et l’écriture des comptes sur disque. Ces courbes décrivent le traitement
sur un poste, au vocabulaire et aux volumes étudiés.

## Exécution

Installer l’[environnement commun](../README.md#réexécuter-les-notebooks), puis lancer toutes les
cellules depuis la racine ou `TP1/`. Le JDK compile [WordCountHadoop.java](WordCountHadoop.java)
avec les bibliothèques Hadoop de PySpark. Le notebook utilise un répertoire temporaire pour le job
et sa sortie. Le [script RDD autonome](wordcount_spark.py) se lance aussi avec
`python TP1/wordcount_spark.py` depuis la racine.

## Expérience HDFS/YARN

Le dossier [`resultats/`](resultats/) conserve les comptes, mesures et journaux de l’expérience
HDFS/YARN initiale avec Hadoop 3.3.6. Ses durées sont distinctes des mesures du mode local ci-dessus.
Pour relancer un job de cette expérience, démarrer [les services Docker](compose.yaml) depuis
`TP1/` et attendre un DataNode actif et un nœud YARN `RUNNING` :

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
