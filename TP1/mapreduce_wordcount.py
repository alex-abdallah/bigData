"""Simulation en mémoire des phases map, shuffle et reduce."""

import argparse
from collections import defaultdict
import json
from pathlib import Path
from time import perf_counter

from wordcount import lire_mots


def mapper(mots):
    return [(mot, 1) for mot in mots]


def regrouper(paires):
    groupes = defaultdict(list)
    for mot, occurrence in paires:
        groupes[mot].append(occurrence)
    return groupes


def reduire(groupes):
    return {mot: sum(valeurs) for mot, valeurs in groupes.items()}


def compter_et_mesurer(chemin):
    debut = perf_counter()
    paires = mapper(lire_mots(chemin))
    fin_map = perf_counter()
    groupes = regrouper(paires)
    fin_shuffle = perf_counter()
    comptes = reduire(groupes)
    fin_reduce = perf_counter()

    return comptes, {
        "Lecture + map": fin_map - debut,
        "Shuffle": fin_shuffle - fin_map,
        "Reduce": fin_reduce - fin_shuffle,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fichier", type=Path)
    arguments = parser.parse_args()
    comptes, _ = compter_et_mesurer(arguments.fichier)
    print(json.dumps(comptes, ensure_ascii=False, sort_keys=True))
