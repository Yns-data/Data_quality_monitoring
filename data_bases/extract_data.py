from datetime import datetime, timedelta
from typing import List 
import os
from Data_quality_monitoring.extract_scripts.extraction_func import get_dataframe
from Data_quality_monitoring.extract_scripts.hashing_rows import hash_row
from Data_quality_monitoring.trans_scripts.db_operations import get_insert_query_from_dataframe
from Data_quality_monitoring.conn_scripts.project_logger import logger
from Data_quality_monitoring.conn_scripts.db_connection import get_db_cursor_connection

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")
TABLENAME = os.getenv("tablename")


def process_data() -> None:
    """
    Processes data for a given date and inserts it into the database.
    
    Args:
        date: Date for which to process data. If None, uses current date.
    """
    try:
        process_date = datetime.now()
        #to populate with fake date

        # def generer_dates(date_debut: str, date_fin: str, pas_heure: int = 1) -> List[datetime]:

        #     debut = datetime.strptime(date_debut, "%Y-%m-%d")
        #     fin = datetime.strptime(date_fin, "%Y-%m-%d")
        #     dates = []

        #     while debut <= fin:
        #         dates.append(debut)
        #         debut += timedelta(hours=pas_heure)

        #     return dates
        # # process_date = generer_dates("2000-01-01", "2005-09-24")
        # process_date = generer_dates("2024-03-01", "2024-05-30")


        logger.info(f"Processing data for date: {process_date}")

        data_dict = get_dataframe(dates=process_date, logger=logger)
        if not data_dict:
            logger.warning("No data retrieved")
            return

        df_pandas = pd.DataFrame(data_dict)
        df_pandas["id"] = df_pandas.apply(hash_row, axis=1)
        df_pandas = df_pandas[df_pandas["id"] != 98205155]

        with get_db_cursor_connection() as cursor:
            table_name = TABLENAME

            query = get_insert_query_from_dataframe(
                df=df_pandas,
                table_name=table_name,
            )
            cursor.execute(query)
            logger.info(f"Data successfully inserted into table {table_name}")

    except Exception as e:
        logger.error(f"Error while processing data: {e}")
        raise

def execute():
    """Main entry point of the script."""
    try:
        logger.info("Starting data processing")
        process_data()
        logger.info("Data processing completed successfully")
    except Exception as e:
        logger.error(f"Error during script execution: {e}")
        raise