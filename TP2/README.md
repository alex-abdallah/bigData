# TP2 — Accidents routiers avec PySpark

**Auteur :** Alex Abdallah · ID16

**Notebook :** [Road_Crash_PySpark.ipynb](Road_Crash_PySpark.ipynb)

L’étude porte sur **127 672 accidents** et **272 591 unités impliquées** en Australie-Méridionale
de 2012 à 2019. Elle reprend les analyses de Rakesh Nain et prolonge l’étude de gravité par une
classification en Spark ML avec un test chronologique.

| Étape | Travail réalisé |
|:---|:---|
| Data | Seize CSV contrôlés par SHA-256 ; lecture avec RDD et DataFrame. |
| EDA | Partitionnement des immatriculations, âge, année de véhicule et gravité selon les signalements. |
| Prétraitement | Contrôle des clés, types numériques, cohérence du nombre de victimes et préparation de la cible. |
| Transformation | Égalité RDD/DataFrame/SQL sur deux requêtes, mesure des temps, évolution annuelle et horaire. |
| ML et Éval | Régression logistique ajustée sur 2012–2017, seuil fixé sur 2018, test sur 2019. |
| Déploiement | Export et rechargement du `PipelineModel` ; prédictions sur un petit CSV sans étiquette. |

Pour la question des permis, on sélectionne les accidents impliquant au moins une unité sans permis.
Compter les lignes de la jointure accidents–unités répéterait les victimes d’un même accident. Les
trois API vérifient les mêmes résultats, y compris les multiplicités. Le modèle de gravité travaille
sur des accidents déjà enregistrés ; le jeu ne renseigne ni l’exposition au trafic ni les trajets sans accident.

**Résultats.** La jointure directe donne 2 072 victimes, contre **2 053** après correction.
Sur 2019, le modèle atteint une AUC PR de **0,135** pour une prévalence des accidents graves ou
mortels de **6,47 %**. Au seuil choisi en validation, il détecte **30,15 %** de ces accidents et
déclenche **1 151** fausses alertes.

## Exécution et fichiers

Installer l’[environnement commun](../README.md#réexécuter-les-notebooks), puis lancer toutes les
cellules depuis la racine ou `TP2/`. Le [dossier `data/`](data/) contient l’instantané, ses
empreintes et le [dictionnaire des champs](data/metadata-for-road-crash.pdf).
Les exports (tableaux, figures, modèle, métriques et prédictions) sont écrits dans
`resultats_road_crash/` à l’exécution.

## Sources

- [Rakesh Nain : analyse des accidents avec PySpark](https://medium.com/rakesh-nain/big-data-analysis-of-road-crash-data-using-pyspark-with-pyspark-tutorial-c78ff2c35588)
- [Instantané du code et des données](https://github.com/RakeshNain/Big-data-Analysis-of-Road-Crash-Data-using-PySpark-with-PySpark-Tutorial/tree/dab8e6466747903bf1e5bd21ce63fea31bfb6dcc)
- [Portail de données de l’Australie-Méridionale](https://data.sa.gov.au/)
