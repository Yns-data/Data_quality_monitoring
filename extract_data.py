from datetime import datetime
import os
from contextlib import contextmanager
from typing import Optional

import duckdb
import pandas as pd
import logging
from extract_scripts.extraction_func import get_dataframe
from trans_scripts.db_operations import get_insert_query_from_dataframe

# Logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(filename)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Configuration
DB_PATH = os.path.join("data_bases", "shops_database.duckdb")
API_BASE_URL = "http://127.0.0.1:8000"

@contextmanager
def get_db_connection():
    """Context manager for database connection handling."""
    conn = None
    try:
        conn = duckdb.connect(DB_PATH)
        yield conn
    except Exception as e:
        logger.error(f"Error while connecting to database: {e}")
        raise
    finally:
        if conn:
            conn.close()
            logger.debug("Database connection closed")

def get_table_name(conn: duckdb.DuckDBPyConnection) -> Optional[str]:
    """Gets the name of the first table in the database."""
    try:
        tables = conn.execute("SHOW TABLES").fetchall()
        if not tables:
            logger.warning("No table found in database")
            return None
        return tables[0][0]
    except Exception as e:
        logger.error(f"Error while retrieving table name: {e}")
        raise

def process_data(date: Optional[datetime] = None) -> None:
    """
    Processes data for a given date and inserts it into the database.
    
    Args:
        date: Date for which to process data. If None, uses current date.
    """
    try:
        # Use current date if none provided
        process_date = date or datetime.now()
        logger.info(f"Processing data for date: {process_date}")

        # Data retrieval
        df = get_dataframe(dates=process_date, logger=logger)
        if df.empty:
            logger.warning("No data retrieved")
            return

        # Convert to pandas DataFrame if needed
        df_pandas = pd.DataFrame(df)

        # Insert into database
        with get_db_connection() as conn:
            table_name = get_table_name(conn)
            if not table_name:
                return

            query = get_insert_query_from_dataframe(
                df=df_pandas,
                table_name=table_name,
            )
            conn.execute(query)
            logger.info(f"Data successfully inserted into table {table_name}")

    except Exception as e:
        logger.error(f"Error while processing data: {e}")
        raise

def main():
    """Main entry point of the script."""
    try:
        logger.info("Starting data processing")
        process_data()
        logger.info("Data processing completed successfully")
    except Exception as e:
        logger.error(f"Error during script execution: {e}")
        raise

if __name__ == "__main__":
    main()