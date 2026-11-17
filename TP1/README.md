# TP1 — Comptage de mots avec Python, Hadoop et PySpark

**Alex Abdallah · ID16** · Polytech Marseille · 17 novembre 2026

[Notebook avec les résultats](TP_ComptageMots.ipynb)

## 1. Objectif et démarche

Un fichier contient une suite de mots et le résultat associe chaque mot à son nombre
d’occurrences. Les quatre méthodes calculent le même dictionnaire. On étudie où se font
le découpage et le regroupement, ce que chaque méthode conserve en mémoire ou écrit sur
disque et l’évolution du temps quand le fichier grandit.

| Méthode | Calcul | Point observé dans le notebook |
|:---|:---|:---|
| Python séquentiel | Lire ligne par ligne et actualiser un dictionnaire. | Durée de parcours et pic d’allocations Python. |
| Python pseudo-MapReduce | Émettre `(mot, 1)`, former les groupes, additionner. | Temps de chaque phase et mémoire des listes intermédiaires. |
| Hadoop MapReduce | Exécuter le WordCount officiel, avec mapper, combiner, reducer et sortie sur disque. | Lignes lues, paires émises, totaux transférés et résultats écrits. |
| PySpark · RDD | `textFile`, `flatMap`, `map`, `reduceByKey`, puis `collect`. | Plan RDD et effet de `local[1]` et `local[4]`. |
| PySpark · DataFrame | `split`, `explode`, puis `groupBy().count()`. | Plan Spark SQL et coût de la persistance pour les agrégations répétées. |

PySpark compte comme une méthode dans le sujet. Ses deux API donnent cinq séries dans les
tableaux de mesure.

## 2. Fichiers et corpus

| Fichier ou dossier | Contenu |
|:---|:---|
| [`TP_ComptageMots.ipynb`](TP_ComptageMots.ipynb) | Protocole, démonstrations, contrôles et résultats chiffrés. |
| [`wordcount.py`](wordcount.py) | Lecture par ligne et comptage séquentiel. |
| [`mapreduce_wordcount.py`](mapreduce_wordcount.py) | Simulation des trois phases et chronométrage par phase. |
| [`hadoop_wordcount.py`](hadoop_wordcount.py) | Appel de l’exemple officiel et lecture de son résultat et de ses compteurs. |
| [`wordcount_spark.py`](wordcount_spark.py) | Construction des plans RDD et DataFrame et script RDD autonome. |
| [`requirements.txt`](requirements.txt) | Versions des dépendances Python. |
| [`compose.yaml`](compose.yaml) et [`config`](config) | Services et paramètres HDFS/YARN. |
| [`data/`](data/) | Corpus de départ et ses répétitions. |
| [`resultats/`](resultats/) | Sorties et journaux de la première série HDFS/YARN. |

| Fichier | Répétitions du bloc | Lignes | Taille décimale |
|:---|---:|---:|---:|
| `mots_m1.txt` | 1 | 1 000 | 7,1 ko |
| `mots_m10.txt` | 10 | 10 000 | 71,2 ko |
| `mots_m100.txt` | 100 | 100 000 | 711,7 ko |
| `mots_m1000.txt` | 1 000 | 1 000 000 | 7,12 Mo |
| `mots_m10000.txt` | 10 000 | 10 000 000 | 71,17 Mo |

Les quatre premiers fichiers sont fournis. Le notebook génère le cinquième en répétant
le premier. Les fichiers ont un mot ASCII par ligne et le même vocabulaire de dix mots.
Le compte de `chat` passe ainsi de 91 à 910 000 entre les deux tailles extrêmes.

## 3. Installation et réexécution

Utiliser Python 3.11 ou 3.12 et Java 17 ou 21. Les commandes sont communes à macOS et Linux.
Sur Windows, la validation a été faite dans Ubuntu via WSL. Pour installer uniquement les
dépendances de ce TP, lancer ces commandes à la racine du dépôt.

```bash
java -version
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r TP1/requirements.txt
python -m ipykernel install --sys-prefix --name python3
```

### Hadoop local

Ouvrir le notebook depuis la racine ou `TP1/`, sélectionner le noyau de `.venv` et exécuter
toutes les cellules. Au premier lancement, le module récupère le JAR d’exemples d’Apache Hadoop
3.4.1, vérifie son SHA-256 et le place dans `TP1/.cache/hadoop/`. Le programme `WordCount` est
déjà compilé dans ce JAR. Java exécute le job et la session Spark. Le moteur `LocalJobRunner`
travaille sur les fichiers locaux. [Source Apache du WordCount](https://github.com/apache/hadoop/blob/rel/release-3.4.1/hadoop-mapreduce-project/hadoop-mapreduce-examples/src/main/java/org/apache/hadoop/examples/WordCount.java)

### Essai HDFS/YARN

Depuis la racine, avec Docker et Compose démarrés, lancer les services et vérifier que HDFS
est sorti du mode sûr et que le NodeManager est `RUNNING`.

```bash
cd TP1
docker compose up -d
docker compose exec -T namenode hdfs dfsadmin -safemode get
docker compose exec -T resourcemanager yarn node -list
TP1_HADOOP_MODE=yarn python -m nbconvert --to notebook --execute \
  --output-dir=/tmp/tp1-hadoop-yarn --ExecutePreprocessor.timeout=1800 \
  --ExecutePreprocessor.kernel_name=python3 TP_ComptageMots.ipynb
docker compose stop
```

| Service | Rôle dans l’expérience |
|:---|:---|
| NameNode | Tient le répertoire des fichiers et localise leurs blocs. |
| DataNode | Stocke et sert les blocs des fichiers d’entrée et de sortie. |
| ResourceManager | Attribue les ressources aux applications. |
| NodeManager | Exécute et supervise les conteneurs de calcul sur le nœud. |

Le notebook copie les fichiers dans un répertoire HDFS propre à l’exécution, lance le WordCount
fourni dans l’image Hadoop 3.3.6, lit `part-r-00000` et supprime le répertoire créé. La commande
écrit le notebook exécuté dans `/tmp/tp1-hadoop-yarn`. Le fichier Compose fixe l’empreinte de
l’image employée pour les mesures. [Guide Apache sur Hadoop à un nœud](https://hadoop.apache.org/docs/r3.4.1/hadoop-project-dist/hadoop-common/SingleCluster.html)

### Scripts autonomes

```bash
python TP1/wordcount.py TP1/data/mots_m1.txt
python TP1/mapreduce_wordcount.py TP1/data/mots_m1.txt
python TP1/wordcount_spark.py
```

## 4. Parcours du notebook

| Sections | Lecture et expériences |
|:---|:---|
| 0 | Préparer les cinq tailles, définir le résultat attendu et créer un petit test avec une tabulation et une ligne vide. |
| 1 et 2 | Compter avec les deux scripts Python. Décomposer le temps du pseudo-MapReduce et comparer les allocations. |
| 3 et 4 | Lancer les jobs Hadoop, rapprocher les sorties aux tailles extrêmes et lire les compteurs du shuffle. |
| 5 | Examiner le plan de chaque API Spark, vérifier le test manuel et alterner les deux API dans les mesures. |
| 6 | Comparer une et quatre tâches concurrentes et mesurer le coût de chargement et de réutilisation du cache Spark. |
| 7 et 8 | Comparer les cinq séries, la dispersion des essais et ce qui explique les écarts. |

## 5. Résultats et interprétation

Le notebook chronomètre trois essais par taille et implémentation, soit 75 vérifications du
dictionnaire complet. Le démarrage de Spark, le chargement du JAR et la copie vers HDFS sont
hors chronométrage. Chaque mesure Hadoop inclut le processus de lancement du job, le traitement,
l’écriture du résultat et sa lecture. Les durées s’appliquent au poste et aux données décrits.

### Durées médianes des cinq implémentations

Le notebook contient l’exécution HDFS/YARN réalisée sous Ubuntu via WSL avec Python 3.12,
Java 21 côté Spark, Spark 4.0.1 et Hadoop 3.3.6 dans Docker. Les durées sont en secondes.

| Lignes | Python séquentiel | Python pseudo-MapReduce | Hadoop HDFS/YARN | PySpark RDD | PySpark DataFrame |
|---:|---:|---:|---:|---:|---:|
| 1 000 | 0,00813 | 0,00636 | 21,97838 | 0,29089 | 0,31312 |
| 10 000 | 0,01368 | 0,01075 | 21,23058 | 0,31485 | 0,30427 |
| 100 000 | 0,05712 | 0,10870 | 22,19113 | 0,32430 | 0,30514 |
| 1 000 000 | 0,55580 | 0,72039 | 23,04358 | 0,62842 | 0,58605 |
| 10 000 000 | 5,55293 | 7,34011 | 29,15862 | 3,11192 | 1,80438 |

Sur 1 000 lignes, les deux versions Python se situent à quelques millisecondes. La médiane la
plus basse est celle du pseudo-MapReduce dans la série HDFS/YARN et celle du séquentiel dans
la série locale. Sur 10 millions de lignes, **PySpark DataFrame** atteint la meilleure médiane
de la série HDFS/YARN à **1,804 s**. Le notebook donne aussi le minimum et le maximum des trois
essais sur ce fichier.

### Lecture des mesures Python

À dix millions de lignes, la lecture et l’émission des paires du pseudo-MapReduce prennent
6,391 s, le regroupement 0,617 s et la réduction
0,033 s, d’après les médianes par phase. Le relevé séparé sur un million de lignes
donne un pic d’allocations Python de **0,026 Mio** en séquentiel
et **113,6 Mio** pour le pseudo-MapReduce.

### Hadoop local et HDFS/YARN

| Lignes | Hadoop local 3.4.1 | Hadoop HDFS/YARN 3.3.6 |
|---:|---:|---:|
| 1 000 | 2,802 | 21,978 |
| 10 000 | 2,790 | 21,231 |
| 100 000 | 2,750 | 22,191 |
| 1 000 000 | 3,809 | 23,044 |
| 10 000 000 | 13,794 | 29,159 |

Le chargement initial dans HDFS est effectué avant les chronos. Les jobs HDFS/YARN ajoutent
la soumission et l’exécution du job dans les services du nœud. Le tableau suivant donne
les compteurs d’un essai sur dix millions d’occurrences pour chaque mode.

| Mode | Paires map | Sorties map transférées | Paires au reducer | Octets transférés | Comptes écrits |
|:---|---:|---:|---:|---:|---:|
| Local | 10 000 000 | 3 | 50 | 623 | 10 |
| HDFS/YARN | 10 000 000 | 1 | 10 | 127 | 10 |

Le fichier de 71,17 Mo tient dans un bloc de 128 Mio sur le HDFS testé, qui donne une seule
sortie map. Le mode local en produit trois. Le combiner agrège les occurrences par mot avant
le transfert et peut intervenir à plusieurs reprises lors des écritures intermédiaires.
Le reducer ramène les totaux partiels aux dix comptes. Les deux modes produisent le même dictionnaire.

### Parallélisme et cache Spark

Pour le RDD, à découpage identique, la médiane passe de **11,185 s**
avec `local[1]` à **3,008 s** avec `local[4]`. En DataFrame, le chargement
du cache prend **2,808 s**. L’agrégation médiane prend ensuite
**0,353 s** depuis les partitions persistées, pour
**1,827 s** depuis le fichier. À partir de ces médianes, le chargement est amorti au bout de **2 agrégations**.

## 6. Limites de la comparaison

Le corpus augmente par répétition et garde dix clés. L’essai renseigne donc sur le coût du
traitement quand les occurrences augmentent. Des fichiers plus variés modifieraient le volume
des dictionnaires, les agrégations partielles et la taille du shuffle. HDFS et YARN tournent sur
un seul poste dans l’essai de ce TP. Le coût de chargement du cache figure séparément. Son intérêt
dépend du nombre de traitements qui réutilisent les mêmes partitions.
