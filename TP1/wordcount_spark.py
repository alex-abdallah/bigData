
import json
import time
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ComptageMots")
    .master("local[2]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

mesures = []

for m in [1, 10, 100, 1000]:
    fichier = f"/tp/data/mots_m{m}.txt"

    debut = time.perf_counter()

    comptes = (
        spark.sparkContext.textFile(fichier)
        .flatMap(lambda ligne: ligne.split())
        .map(lambda mot: (mot, 1))
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )

    duree = time.perf_counter() - debut

    mesures.append({
        "m": m,
        "temps_s": duree,
        "comptes": dict(comptes)
    })

spark.stop()

print("RESULTATS_JSON=" + json.dumps(mesures))
