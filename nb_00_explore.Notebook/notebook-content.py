# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1a032321-a1eb-44a4-8cbf-79c7f59eaa8a",
# META       "default_lakehouse_name": "lkh_atlasnet",
# META       "default_lakehouse_workspace_id": "5308aa28-eb93-41df-9239-ed8ff3a2c753",
# META       "known_lakehouses": [
# META         {
# META           "id": "1a032321-a1eb-44a4-8cbf-79c7f59eaa8a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

file_path = "Files/landing/cdr/cdr_2026-08-01.csv.gz"
df = spark.read.csv(file_path, header=True, inferSchema=True)
display(df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# afficher le nombre des lignes 
df.count()
# verifier si la colonne une colonne contient des doublons
def colonne_est_unique(df:DataFrame, nom_colonne:str):
    nombre_lignes = df.count()
    nombre_valeurs_distinct = df.agg(F.countDistinct(nom_colonne)).first()[0]
    if nombre_lignes == nombre_valeurs_distinct :
        return True
    else:
        return False
# colonne_est_unique(df,"record_id")

# verifier si une colonne contien des valeur NULL ou non
def nombre_valeurs_NULL(df:DataFrame, nom_colonne:str):
    nombre_valeur_NULL = df.filter(F.col(nom_colonne).isNull()).count()
    return nombre_valeur_NULL

nombre_valeurs_NULL(df,"record_id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
