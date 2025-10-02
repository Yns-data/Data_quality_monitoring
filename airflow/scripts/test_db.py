# type: ignore
from data_quality_monitoring.data_bases.extract_data_to_gcp import execute
from data_quality_monitoring.conn_scripts.project_logger import logger
from dotenv import load_dotenv
import datetime
import os

load_dotenv()

KEY = os.getenv("key")
PROJECT = os.getenv("project_id")
DATASET = os.getenv("dataset_id")
TABLE = os.getenv("table_id")
BASE_URL = os.getenv("API_BASE_URL")
logger.info(f"Starting data extraction and loading process at {datetime.datetime.now()}")
execute(PROJECT, DATASET, TABLE, BASE_URL, KEY)
logger.info(f"Ending data extraction and loading process at {datetime.datetime.now()}")
