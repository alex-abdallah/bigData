"""Comptage séquentiel en lisant un fichier ligne à ligne."""

import argparse
import json
from pathlib import Path


def lire_mots(chemin):
    with Path(chemin).open(encoding="utf-8") as fichier:
        for ligne in fichier:
            yield from ligne.split()


def compter(chemin):
    comptes = {}
    for mot in lire_mots(chemin):
        comptes[mot] = comptes.get(mot, 0) + 1
    return comptes


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fichier", type=Path)
    arguments = parser.parse_args()
    print(json.dumps(compter(arguments.fichier), ensure_ascii=False, sort_keys=True))
