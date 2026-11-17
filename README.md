# Big Data — Rendu des TP 1, 2 et 3

**Alex Abdallah · ID16** — Polytech Marseille, FISE 5A Informatique

| Rendu | Sujet | Notebook avec résultats | Documentation |
|:---|:---|:---|:---|
| [TP1](TP1/) | Comptage de mots : Python séquentiel, PySpark, Python pseudo-MapReduce et Hadoop MapReduce | [Comptage de mots](TP1/TP_ComptageMots.ipynb) | [Protocole et données](TP1/README.md) |
| [TP2](TP2/) | Accidents routiers en Australie-Méridionale avec PySpark | [Accidents routiers](TP2/Road_Crash_PySpark.ipynb) | [Sources et déroulé](TP2/README.md) |
| [TP3_ID16/Small](TP3_ID16/Small/) | Fraude bancaire : traitement Spark et pandas, classification scikit-learn | [Fraude bancaire](TP3_ID16/Small/TP3_small_fraude_bancaire.ipynb) | [Protocole Small](TP3_ID16/README.md#small--fraude-bancaire) |
| [TP3_ID16/Big](TP3_ID16/Big/) | Sentiment sur quatre millions d’avis Amazon avec PySpark | [Avis Amazon](TP3_ID16/Big/TP3_big_sentiment_amazon_spark.ipynb) | [Protocole Big](TP3_ID16/README.md#big--sentiment-des-avis-amazon) |

Les tableaux et figures sont enregistrés dans les notebooks pour une lecture directe sur GitHub.
Le TP1 s’ouvre sur le titre, la date et l’identification de l’auteur. Les trois autres notebooks
commencent par la contribution et suivent les titres `## [Data]`, `## [EDA]`,
`## [Prétraitement]`, `## [Transformation]`, `## [ML]`, `## [Éval]` et `## [Déploiement]`.
Chaque notebook se termine par une conclusion et les limites de l’étude.

Les quatre notebooks ont été vérifiés de bout en bout sous Ubuntu WSL (Python 3.12, Java 21,
Spark 4.0.1), sur les données complètes. Les résultats sont détaillés dans les README de chaque TP.

## Réexécuter les notebooks

Utiliser **Linux ou Ubuntu sous WSL**, **Python 3.11 ou 3.12** et un **JDK 17 ou 21** (`java` et `javac`).
La version de [Spark utilisée est 4.0.1](https://spark.apache.org/docs/4.0.1/).
Les données nécessaires sont fournies dans le dépôt ; le clonage représente près de 900 Mo de données.
Les notebooks n’ont pas besoin de compte Kaggle ni de conteneur Docker pour l’exécution standard.

Depuis la racine du dépôt :

```bash
java -version
javac -version
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m ipykernel install --sys-prefix --name python3
```

Dans Jupyter ou VS Code, ouvrir les notebooks avec le noyau de `.venv`, puis lancer toutes les
cellules. Exécuter les notebooks l’un après l’autre. Le TP3 Big utilise tout le corpus par défaut ;
prévoir environ 12 Go de mémoire disponible et plusieurs gigaoctets libres pour Spark.

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

La présentation du rendu s’inspire du [dépôt de Batist Aureille](https://github.com/Batigoal13/BigData/tree/main).
Les sources et les notebooks de départ de chaque étude sont crédités dans leurs dossiers respectifs.
