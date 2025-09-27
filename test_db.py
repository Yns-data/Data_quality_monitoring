# type: ignore
from data_quality_monitoring.data_bases.extract_data_to_gcp import execute
from dotenv import load_dotenv
import os

load_dotenv()

KEY = os.getenv("key")
PROJECT = os.getenv("project_id")
DATASET = os.getenv("dataset_id")
TABLE = os.getenv("table_id")
BASE_URL = os.getenv("API_BASE_URL")
execute(PROJECT, DATASET, TABLE, BASE_URL, KEY)