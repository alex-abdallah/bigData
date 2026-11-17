# TP2 — Accidents routiers en Australie-Méridionale avec PySpark

**Alex Abdallah · ID16** · Polytech Marseille, FISE 5A Informatique

L’étude examine **127 672 accidents enregistrés** et **272 591 unités impliquées** entre 2012 et 2019.
Elle vérifie deux requêtes avec les RDD, les DataFrames et Spark SQL, décrit les différences de gravité
selon les circonstances et mesure ce qu’un modèle peut apprendre du contexte. Le notebook contient les
tableaux, les graphiques, les résultats de validation et les plans Spark résumés.
[Ouvrir le notebook](Road_Crash_PySpark.ipynb)

## 1. Fichiers du rendu

```text
TP2/
├── README.md                      Résultats et procédure de réexécution
├── Road_Crash_PySpark.ipynb         Notebook exécuté, de la lecture au benchmark
├── data/
│   ├── crash_data/                 8 CSV annuels, une ligne par accident
│   ├── units_data/                 8 CSV annuels, une ligne par unité impliquée
│   ├── empreintes.json             Empreintes SHA-256 des 16 CSV
│   └── metadata-for-road-crash.pdf   Dictionnaire des variables
├── modele_gravite_spark/            PipelineModel PySpark utilisable pour la prédiction
└── exemple_contexte.csv            5 lignes d’entrée pour tester le modèle
```

Un accident est identifié par `REPORT_ID`. Une unité est identifiée par le couple
`REPORT_ID` et `Unit No`. Elle peut représenter un véhicule, un piéton ou un autre objet impliqué.
Le regroupement des fichiers sources sous `data/` permet de vérifier, en un seul endroit, les données
utilisées par le notebook. Les définitions et les codes de gravité figurent dans le dictionnaire.
[Dictionnaire des champs](data/metadata-for-road-crash.pdf)

## 2. Parcours et corrections

| Étape | Traitement et vérification |
|:---|:---|
| `[Data]` | Valider les empreintes et les en-têtes des 16 CSV. Comparer les effectifs des lecteurs RDD et DataFrame. |
| `[Prétraitement]` | Vérifier les deux clés et rapprocher le nombre d’unités et de victimes des totaux d’accident. Construire l’heure, la vitesse limite et la cible. |
| `[EDA]` | Mesurer les effectifs et les parts de gravité par année, type, jour, heure, signalement, vitesse et conditions. |
| `[Transformation]` | Vérifier que les trois API renvoient les mêmes lignes à Adelaide et pour les accidents impliquant une unité sans permis. |
| `[ML]` | Entraîner deux modèles jusqu’en 2017. Retenir le meilleur selon l’AUC PR sur 2018 et fixer le seuil de décision sur cette même période. |
| `[Éval]` | Évaluer le modèle sur 2019, afficher la matrice de confusion et analyser les erreurs par condition de lumière. |
| `[Déploiement]` | Sauvegarder le pipeline complet, le recharger et vérifier les scores et les décisions sur cinq contextes. |
| `[Benchmark]` | Refaire la requête du permis à un et quatre cœurs, puis faire varier les partitions de shuffle en contrôlant les sorties. |
| `[Conclusion]` | Résumer les corrections, les résultats et le périmètre des mesures. |

| Problème dans la démarche initiale | Correction appliquée |
|:---|:---|
| La virgule peut faire partie d’un champ entre guillemets. | Le parseur CSV préserve le champ. Un découpage brut aurait déplacé les colonnes de 21 379 lignes d’accident, soit 16,75 %. |
| La jointure accident–unité répète les victimes si plusieurs unités sans permis partagent un `REPORT_ID`. | La requête retient chaque accident une fois. Le total passe de **2 072 à 2 053 victimes**. Les trois API reproduisent le tableau corrigé par localité. |
| `Day` désigne un jour de la semaine. | Les requêtes conservent `Year`, `Month`, `Day` et `Time` dans des champs distincts et confrontent les lignes avec leurs multiplicités. |
| Une comparaison de gravité peut mélanger les grains accident et unité. | Les graphiques d’accidents sont calculés sur la table principale. Les résultats par permis ou type d’unité indiquent combien d’unités sont associées à un accident grave. |

## 3. Résultats de l’analyse

| Indicateur | Observation sur l’instantané |
|:---|:---|
| Bilan et gravité | 53 255 victimes, dont 781 décès. On compte 5 597 accidents graves ou mortels sur 127 672. |
| Décès selon le type de collision | Les chocs contre un obstacle fixe totalisent le plus de décès (245). Les collisions frontales ont le taux le plus élevé parmi les types comptant au moins 200 accidents (65,20 décès pour 1 000 accidents). |
| Variation selon l’heure | Le volume culmine à 17 h (11 642 accidents). La plus forte part de cas graves ou mortels se situe à 2 h (9,47 % sur 940 accidents). |

Les taux par vitesse limite sont accompagnés d’intervalles de Wilson à 95 % et calculés pour les
valeurs comptant au moins 200 accidents. La vitesse limite désigne la vitesse autorisée sur le lieu
de l’accident. Les signalements d’alcool et de stupéfiants sont décrits avec leurs effectifs et leurs
parts de gravité. Ces résultats portent sur les accidents enregistrés, avec les conditions et les
signalements disponibles dans les CSV.

## 4. Modèle de gravité

La cible vaut 1 pour les accidents classés `3: SI` ou `4: Fatal`. Les six entrées sont `Time`,
`Month`, `Day`, `DayNight`, `Stats Area` et `Area Speed`. Le pipeline transforme l’heure et la
vitesse, traite les catégories inconnues, assemble le vecteur de variables puis calcule le score.
Les entrées se limitent aux six variables de contexte.

| Période | Utilisation |
|:---|:---|
| 2012–2017 | Apprendre les transformations et entraîner une régression logistique et des arbres boostés. |
| 2018 | Comparer l’AUC précision-rappel des deux modèles. Parmi 99 valeurs possibles, fixer le seuil du modèle retenu en maximisant le F1. |
| 2019 | Mesurer la précision, le rappel, le F1, l’AUC PR et l’AUC ROC avec le seuil déjà fixé. |

Le test contient **839 accidents graves ou mortels** parmi **12 964 accidents**. La prévalence de
**6,47 %** sert de repère pour lire l’AUC PR. Sur 2018, les arbres boostés obtiennent une AUC PR de
**0,1182** et la régression logistique **0,1123**. Le modèle d’arbres est retenu avec un seuil de **0,14**
qui donne un F1 de 0,2312 sur 2018.

| Résultat sur 2019 | Valeur |
|:---|---:|
| Aire sous la courbe précision-rappel (AUC PR) | 0,1352 |
| Aire sous la courbe ROC | 0,6582 |
| Précision | 20,08 % |
| Rappel | 24,08 % |
| F1 | 0,2190 |

| Classe réelle | Autre prédit | Grave prédit |
|:---|---:|---:|
| Autre | 11 321 | 804 |
| Grave ou mortel | 637 | 202 |

Le modèle détecte **202 cas graves sur 839** et déclenche **804 fausses alertes**. La règle qui prédit
toujours la classe majoritaire atteint **93,53 % d’exactitude** et laisse passer les 839 cas graves.
La matrice de confusion montre le compromis au seuil publié. Le détail par condition de lumière
figure dans le notebook.

## 5. Comparaison des API Spark

Les entrées sont projetées sur les colonnes utiles, mises en cache sur huit partitions et utilisées
par les trois API. Pour chaque mode local, un essai d’échauffement précède trois mesures par API.
Le chronomètre couvre la construction et l’exécution de la requête jusqu’à la collecte du résultat
agrégé. L’égalité du résultat par localité est contrôlée après chaque essai.

La comparaison `local[1]` et `local[4]` maintient les réglages de jointure et de shuffle. Le balayage
de **4, 8, 32 et 128 partitions** se fait ensuite à quatre cœurs, avec la diffusion et l’exécution
adaptative désactivées pour observer l’effet du shuffle.

| API | Médiane à 1 cœur (s) | Médiane à 4 cœurs (s) |
|:---|---:|---:|
| RDD | 8,064 | 2,370 |
| DataFrame | 0,421 | 0,201 |
| Spark SQL | 0,402 | 0,156 |

Les plans DataFrame et SQL affichent une jointure par diffusion (`BroadcastHashJoin`) et une
agrégation par hachage. Dans le balayage avec redistribution des clés, la médiane passe de
**0,246 s à 8 partitions** à **0,943 s à 128 partitions**. Les plages des essais à 4 et 8 partitions
se recoupent. Ces durées concernent les entrées mises en cache et le mode local de cette exécution.
[Documentation des réglages Spark](https://spark.apache.org/docs/4.0.1/sql-performance-tuning.html)

## 6. Réexécuter le TP2

Installer les dépendances communes et utiliser le noyau de l’environnement Python. L’exécution
complète a été vérifiée avec Python 3.12, Java 21 et Spark 4.0.1 sous Ubuntu via WSL. Lancer
toutes les cellules du notebook depuis le dossier `TP2/` ou la racine du dépôt.
[Installation et réexécution](../README.md#5-réexécuter-les-notebooks)

Depuis la racine, on peut également exécuter le notebook en ligne de commande.

```bash
python -m nbconvert --to notebook --execute --output-dir=/tmp/tp2-verification \
  --ExecutePreprocessor.timeout=1200 --ExecutePreprocessor.kernel_name=python3 \
  TP2/Road_Crash_PySpark.ipynb
```

Les graphiques et exports sont écrits dans `TP2/resultats_road_crash/`. Le dossier de sortie
contient le modèle réentraîné, le lot d’exemple, les scores du lot, les métriques sur 2018 et 2019
et deux CSV de benchmark. `BIGDATA_OUTPUT_ROOT` permet de placer ces fichiers ailleurs, dans un
sous-dossier `TP2/`. Le modèle fourni à côté du notebook correspond à l’exécution publiée.

### Recharger le modèle fourni

Ce bloc peut être exécuté depuis la racine ou `TP2/`, avec le même environnement Python.
Les données d’exemple contiennent uniquement les six colonnes nécessaires. Le pipeline renvoie
un score associé à la classe grave et une décision binaire au seuil fixé sur 2018.

```python
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession

tp2 = Path("TP2") if Path("TP2/modele_gravite_spark").is_dir() else Path.cwd()
with SparkSession.builder.master("local[2]").appName("TP2 - Exemple").getOrCreate() as spark:
    modele = PipelineModel.load((tp2 / "modele_gravite_spark").resolve().as_uri())
    exemple = spark.read.csv((tp2 / "exemple_contexte.csv").resolve().as_uri(), header=True)
    resultat = modele.transform(exemple)
    resultat.select("Time", "DayNight", "Area Speed",
                    vector_to_array("probability")[1].alias("score_grave"), "prediction").show()
```

## Sources

- Analyse de départ et jeux de données annuels. [Rakesh Nain](https://medium.com/rakesh-nain/big-data-analysis-of-road-crash-data-using-pyspark-with-pyspark-tutorial-c78ff2c35588) · [Instantané du code et des données](https://github.com/RakeshNain/Big-data-Analysis-of-Road-Crash-Data-using-PySpark-with-PySpark-Tutorial/tree/dab8e6466747903bf1e5bd21ce63fea31bfb6dcc)
- Données publiques d’Australie-Méridionale. [Portail Data.SA](https://data.sa.gov.au/)
- Sauvegarde et rechargement d’un pipeline. [Guide Spark ML 4.0.1](https://spark.apache.org/docs/4.0.1/ml-pipeline.html)
