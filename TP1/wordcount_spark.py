
import json
import os
from pathlib import Path
import sys
import time
from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = sys.executable
data = Path(__file__).resolve().parent / "data"
spark = (
    SparkSession.builder
    .appName("ComptageMots")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

mesures = []

try:
    for m in [1, 10, 100, 1000]:
        fichier = (data / f"mots_m{m}.txt").as_uri()
        debut = time.perf_counter()
        comptes = (
            spark.sparkContext.textFile(fichier)
            .flatMap(str.split)
            .map(lambda mot: (mot, 1))
            .reduceByKey(lambda a, b: a + b)
            .collect()
        )
        mesures.append({"m": m, "temps_s": time.perf_counter() - debut, "comptes": dict(comptes)})
finally:
    spark.stop()

print("RESULTATS_JSON=" + json.dumps(mesures))
