#!/usr/bin/env python3
"""
create_bq_table.py

Usage:
  python create_bq_table.py --key /chemin/vers/cle.json --project mon-projet --dataset mon_dataset --table ma_table

Ce script :
 - s'authentifie avec une clé JSON de service account
 - crée le dataset si nécessaire
 - crée la table avec un schéma d'exemple (modifiable)
 - ne plante pas si la table existe déjà
"""

import argparse
import sys
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import Conflict, NotFound

def parse_args():
    parser = argparse.ArgumentParser(description="Créer une table BigQuery avec authentification JSON.")
    parser.add_argument("--key", help="Chemin vers le fichier JSON de la service account")
    parser.add_argument("--project", help="ID du projet GCP")
    parser.add_argument("--dataset", help="Nom du dataset (ex: mon_dataset)")
    parser.add_argument("--table", help="Nom de la table à créer (ex: ma_table)")
    parser.add_argument("--interactive", action="store_true", help="Mode interactif")
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Mode interactif si pas d'arguments
    if not any([args.key, args.project, args.dataset, args.table]):
        args.key = "noted-cider-471206-n4-6f7e8536f07f.json"
        args.project = "noted-cider-471206-n4"
        args.dataset = "data_monitoring"
        args.table = "table_test"

    # Charger les credentials depuis le fichier JSON
    credentials = service_account.Credentials.from_service_account_file(args.key)

    # Créer le client BigQuery
    client = bigquery.Client(project=args.project, credentials=credentials)

    dataset_id = f"{args.project}.{args.dataset}"
    table_id = f"{dataset_id}.{args.table}"

    # Exemple de schéma — adapte selon tes besoins
    schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("nom", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("score", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
    ]

    # 1) Créer le dataset s'il n'existe pas
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "EU"  # adapte la localisation si besoin (ex: "US", "EU")
    try:
        client.create_dataset(dataset)  # API request
        print(f"Dataset {dataset_id} créé.")
    except Conflict:
        print(f"Dataset {dataset_id} existe déjà — ok.")

    # 2) Définir la table et créer
    table = bigquery.Table(table_id, schema=schema)
    # Optionnel : configurer la partition si tu veux partitionnement par date
    # table.time_partitioning = bigquery.TimePartitioning(field="created_at")

    try:
        created_table = client.create_table(table)  # API request
        print(f"Table {created_table.project}.{created_table.dataset_id}.{created_table.table_id} créée avec succès.")
    except Conflict:
        print(f"La table {table_id} existe déjà — création ignorée.")
    except Exception as e:
        print("Erreur lors de la création de la table :", str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()
