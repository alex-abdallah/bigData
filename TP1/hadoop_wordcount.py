"""Exécuter le WordCount officiel d’Apache Hadoop et lire les compteurs du job."""

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
from tempfile import NamedTemporaryFile, TemporaryDirectory
from urllib.request import urlopen
from uuid import uuid4

import pyspark


VERSION = "3.4.1"
ARTIFACT = f"hadoop-mapreduce-examples-{VERSION}.jar"
URL = ("https://repo.maven.apache.org/maven2/org/apache/hadoop/"
       f"hadoop-mapreduce-examples/{VERSION}/{ARTIFACT}")
SHA256 = "8eb53c7e38c4ce7cafe10f527ed30064f8aa461ac3e03d02567cc4295e42e020"
COMPTEURS = (
    "Map input records", "Map output records", "Combine input records",
    "Combine output records", "Shuffled Maps", "Reduce shuffle bytes",
    "Reduce input records", "Reduce output records",
)
CONFIG_JOURNAL = """status = warn
appender.console.type = Console
appender.console.name = Console
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %m%n
rootLogger.level = warn
rootLogger.appenderRef.console.ref = Console
logger.job.name = org.apache.hadoop.mapreduce.Job
logger.job.level = info
"""


@dataclass(frozen=True)
class Rapport:
    comptes: dict[str, int]
    compteurs: dict[str, int]


def empreinte(fichier):
    with fichier.open("rb") as flux:
        return hashlib.file_digest(flux, "sha256").hexdigest()


def obtenir_exemple(cache):
    cache.mkdir(parents=True, exist_ok=True)
    jar = cache / ARTIFACT
    if jar.is_file() and empreinte(jar) == SHA256:
        return jar

    temporaire = None
    try:
        with NamedTemporaryFile(dir=cache, suffix=".tmp", delete=False) as fichier:
            temporaire = Path(fichier.name)
            with urlopen(URL, timeout=30) as reponse:
                shutil.copyfileobj(reponse, fichier)
        if empreinte(temporaire) != SHA256:
            raise ValueError("Le JAR reçu diffère de l’exemple Apache Hadoop 3.4.1 attendu.")
        temporaire.replace(jar)
    finally:
        if temporaire is not None:
            temporaire.unlink(missing_ok=True)
    return jar


def executer(commande, *, cwd=None, stdin=None, timeout=180):
    resultat = subprocess.run(commande, cwd=cwd, stdin=stdin, capture_output=True,
                              text=True, encoding="utf-8", errors="replace", timeout=timeout)
    if resultat.returncode:
        details = (resultat.stdout + "\n" + resultat.stderr)[-5000:].strip()
        raise RuntimeError(f"Échec de {commande[0]} (code {resultat.returncode}).\n{details}")
    return resultat


def lire_rapport(sortie, journal):
    comptes = {}
    for ligne in sortie.splitlines():
        mot, nombre = ligne.split("\t", maxsplit=1)
        comptes[mot] = int(nombre)

    valeurs = re.findall(r"^[ \t]*([^\n=]+?)[ \t]*=[ \t]*(\d+)[ \t]*$", journal, re.MULTILINE)
    compteurs = {nom.strip(): int(nombre) for nom, nombre in valeurs if nom.strip() in COMPTEURS}
    manquants = set(COMPTEURS) - compteurs.keys()
    if manquants:
        raise RuntimeError(f"Compteurs Hadoop absents du rapport final : {', '.join(sorted(manquants))}")
    return Rapport(comptes, compteurs)


@contextmanager
def hadoop_local(dossier_tp):
    jar = obtenir_exemple(dossier_tp / ".cache" / "hadoop")
    jars_spark = Path(pyspark.__file__).resolve().parent / "jars"
    if not (jars_spark / f"hadoop-client-runtime-{VERSION}.jar").is_file():
        raise RuntimeError("Installer PySpark 4.0.1 pour utiliser les bibliothèques Hadoop 3.4.1.")

    java_home = os.environ.get("JAVA_HOME")
    java = Path(java_home) / "bin" / ("java.exe" if os.name == "nt" else "java") if java_home else None
    lanceur = str(java) if java and java.is_file() else shutil.which("java")
    if not lanceur:
        raise RuntimeError("Rendre Java 17 ou 21 accessible dans le PATH ou via JAVA_HOME.")

    with TemporaryDirectory(prefix="tp1-hadoop-") as repertoire:
        travail = Path(repertoire)
        config = travail / "log4j2.properties"
        config.write_text(CONFIG_JOURNAL, encoding="utf-8")
        classpath = os.pathsep.join((str(jar), str(jars_spark / "*")))
        commande = [lanceur, "-Xmx768m", f"-Dlog4j2.configurationFile={config.as_uri()}",
                    "-cp", classpath, "org.apache.hadoop.examples.WordCount",
                    "-Dmapreduce.framework.name=local", "-Dfs.defaultFS=file:///",
                    "-Dmapreduce.job.reduces=1"]

        def compter(chemin):
            with TemporaryDirectory(dir=travail) as essai:
                resultat = Path(essai) / "comptes"
                job = executer([*commande, chemin.resolve().as_uri(), resultat.as_uri()])
                sortie = (resultat / "part-r-00000").read_text(encoding="utf-8")
                return lire_rapport(sortie, job.stdout + "\n" + job.stderr)

        yield f"Hadoop {VERSION} · LocalJobRunner", compter


@contextmanager
def hadoop_yarn(dossier_tp, fichiers):
    docker = shutil.which("docker") or shutil.which("docker.exe")
    if not docker:
        raise RuntimeError("Le mode yarn utilise le cluster Docker Compose du dossier TP1.")

    def service(conteneur, *arguments, stdin=None, timeout=180):
        commande = [docker, "compose", "exec", "-T", conteneur, *arguments]
        return executer(commande, cwd=dossier_tp, stdin=stdin, timeout=timeout)

    version = service("namenode", "hadoop", "version", timeout=30).stdout.splitlines()[0]
    safemode = service("namenode", "hdfs", "dfsadmin", "-safemode", "get", timeout=30).stdout
    nodes = service("resourcemanager", "yarn", "node", "-list", timeout=30).stdout
    if "Safe mode is OFF" not in safemode or "RUNNING" not in nodes:
        raise RuntimeError("Attendre la disponibilité de HDFS et d’au moins un nœud YARN RUNNING.")

    racine = f"/tmp/tp1-wordcount-{uuid4().hex}"
    service("namenode", "hdfs", "dfs", "-mkdir", "-p", f"{racine}/entrees")
    try:
        sources = {}
        for i, chemin in enumerate(fichiers, start=1):
            entree = f"{racine}/entrees/fichier-{i}.txt"
            with chemin.open("rb") as flux:
                service("namenode", "hdfs", "dfs", "-put", "-", entree, stdin=flux)
            sources[chemin] = entree

        def compter(chemin):
            resultat = f"{racine}/sortie-{uuid4().hex}"
            job = service(
                "namenode", "bash", "-c",
                'exec hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar '
                'wordcount -Dmapreduce.job.reduces=1 "$1" "$2"',
                "wordcount", sources[chemin], resultat, timeout=300,
            )
            sortie = service("namenode", "hdfs", "dfs", "-cat", f"{resultat}/part-r-00000").stdout
            return lire_rapport(sortie, job.stdout + "\n" + job.stderr)

        yield f"{version} · HDFS/YARN", compter
    finally:
        service("namenode", "hdfs", "dfs", "-rm", "-r", "-skipTrash", racine, timeout=30)
