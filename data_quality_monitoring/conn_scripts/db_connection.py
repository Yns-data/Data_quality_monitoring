from contextlib import contextmanager
import psycopg2
from dotenv import load_dotenv
from data_quality_monitoring.conn_scripts.project_logger import logger
import os
from pathlib import Path

current_dir = Path(__file__).parent.parent
env_path = current_dir / '.env'

logger.debug(f"Looking for .env file at: {env_path}")
load_success = load_dotenv(env_path)
logger.debug(f"Load dotenv success: {load_success}")
logger.debug(f"Environment variables loaded - USER: {os.getenv('user')}, HOST: {os.getenv('host')}")

USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")
TABLENAME = os.getenv("tablename")
@contextmanager
def get_db_cursor_connection():
    """Context manager for database connection handling."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            dbname=DBNAME
        )
        cursor = conn.cursor()
        logger.info("Database connection successful")
        yield cursor
        conn.commit()
    except Exception as e:
        logger.error(f"Error while connecting to or operating on the database: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception as e:
                logger.error(f"Error while closing cursor: {e}")
        if conn:
            try:
                conn.close()
                logger.debug("Database connection closed")
            except Exception as e:
                logger.error(f"Error while closing database connection: {e}")

@contextmanager
def get_db_connection():
    """Context manager for database connection handling."""
    conn = None
    try:
        conn = psycopg2.connect(
            user=USER,
            password=PASSWORD,
            host=HOST,
            port=PORT,
            dbname=DBNAME
        )
        logger.info("Database connection successful")
        yield conn
    except Exception as e:
        logger.error(f"Error while connecting to or operating on the database: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            try:
                conn.close()
                logger.debug("Database connection closed")
            except Exception as e:
                logger.error(f"Error while closing database connection: {e}")
