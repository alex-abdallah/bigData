# Big Data · Travaux pratiques

**Alex Abdallah** · Polytech Marseille · FISE 5A Informatique

Trois travaux pratiques : comparer des moteurs de calcul, analyser des données avec Spark et construire deux projets data de la lecture des données à la prédiction.

| TP | Sujet | Notebooks |
|:---|:---|:---|
| [TP1 · Comptage de mots](TP1/) | Python, pseudo-MapReduce, Hadoop et PySpark (RDD et DataFrame) sur quatre volumes. | [Comptage de mots](TP1/TP_ComptageMots.ipynb) |
| [TP2 · Accidents routiers](TP2/) | Accidents en Australie-Méridionale de 2012 à 2019, avec comparaison des API Spark et analyse de la gravité. | [Road Crash](TP2/Road_Crash_PySpark.ipynb) |
| [TP3 · Cycle d’un projet data](TP3/) | Fraude bancaire avec pandas et scikit-learn ; sentiment de quatre millions d’avis Amazon avec Spark. | [Small · Fraude](TP3/small/TP3_small_fraude_bancaire.ipynb) · [Big · Avis Amazon](TP3/big/TP3_big_sentiment_amazon_spark.ipynb) |

## Lancer les notebooks

Utiliser **Python 3.11 ou 3.12** et **Java 17 ou 21**. Pour les notebooks Spark sous Windows, utiliser **Ubuntu dans WSL** et ouvrir le dossier avec VS Code en mode WSL. La lecture des données se fait depuis le dépôt : aucun compte Kaggle ni téléchargement de CSV dans les notebooks.

Depuis le dossier `bigData/`, sous Linux ou WSL :

```bash
java -version
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m ipykernel install --sys-prefix --name python3
```

Ouvrir un notebook dans VS Code (extensions Python et Jupyter), sélectionner le noyau Python de `.venv`, puis cliquer sur **Run All**. Lancer les notebooks un par un. Prévoir plusieurs gigaoctets libres pour les fichiers intermédiaires Spark et, pour le TP3 Big, un environnement disposant d’environ **12 Go de mémoire disponible**. L’étude Big utilise le corpus complet par défaut.

<details>
<summary>Vérifier les quatre notebooks depuis le terminal</summary>

```bash
python -m nbconvert --to notebook --execute --output-dir=/tmp/bigdata-verification \
  --ExecutePreprocessor.timeout=3600 --ExecutePreprocessor.kernel_name=python3 \
  TP1/TP_ComptageMots.ipynb \
  TP2/Road_Crash_PySpark.ipynb \
  TP3/small/TP3_small_fraude_bancaire.ipynb \
  TP3/big/TP3_big_sentiment_amazon_spark.ipynb
```

Les copies exécutées vont dans `/tmp/bigdata-verification`. Les modèles et les résultats sont générés à côté de leur notebook.

</details>

**Vérification :** les quatre notebooks ont été exécutés du début à la fin sous Ubuntu WSL avec Python 3.12, Java 21 et Spark 4.0.1. Le test du TP3 Big a utilisé les quatre millions d’avis en entrée, avec les réglages par défaut.

**TP1 :** le mode par défaut recalcule les deux méthodes Python et les deux méthodes PySpark. Il relit les résultats Hadoop de l’expérience originale et vérifie leurs comptes mot par mot. Le [README du TP1](TP1/README.md) explique comment refaire les mesures Hadoop avec Docker.

**TP3 Big :** pour une démonstration plus courte, définir `TP3_BIG_FRACTION=0.02` avant de démarrer le noyau. Les résultats de cet essai sont écrits dans un dossier séparé.
