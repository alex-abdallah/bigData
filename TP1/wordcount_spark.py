
"""Comptage PySpark en API RDD ou DataFrame."""

import json
from operator import add
import os
from pathlib import Path
import sys
from time import perf_counter

from pyspark.sql import SparkSession, functions as F


PARTITIONS = 4


def demarrer_spark(nombre_coeurs):
    os.environ["PYSPARK_PYTHON"] = sys.executable
    spark = (
        SparkSession.builder.appName("TP1 - Comptage de mots")
        .master(f"local[{nombre_coeurs}]")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", str(PARTITIONS))
        .config("spark.sql.files.maxPartitionBytes", "16m")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def plan_rdd(spark, chemin):
    lignes = spark.sparkContext.textFile(chemin.resolve().as_uri(), minPartitions=PARTITIONS)
    return lignes.flatMap(str.split).map(lambda mot: (mot, 1)).reduceByKey(add, PARTITIONS)


def mots_dataframe(spark, chemin):
    lignes = spark.read.text(chemin.resolve().as_uri())
    mots = lignes.select(F.explode(F.split("value", r"\s+")).alias("mot"))
    return mots.where(F.col("mot") != "")


def plan_dataframe(mots):
    return mots.groupBy("mot").count()


def compter_dataframe(mots):
    return {ligne.mot: ligne["count"] for ligne in plan_dataframe(mots).collect()}


if __name__ == "__main__":
    data = Path(__file__).resolve().parent / "data"
    spark = demarrer_spark(2)
    mesures = []
    try:
        for m in (1, 10, 100, 1000):
            chemin = data / f"mots_m{m}.txt"
            debut = perf_counter()
            comptes = dict(plan_rdd(spark, chemin).collect())
            mesures.append({"m": m, "temps_s": perf_counter() - debut, "comptes": comptes})
    finally:
        spark.stop()
    print("RESULTATS_JSON=" + json.dumps(mesures))
