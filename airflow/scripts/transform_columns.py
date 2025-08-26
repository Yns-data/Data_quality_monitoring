from data_quality_monitoring.conn_scripts.db_connection import get_db_connection
from data_quality_monitoring.conn_scripts.project_logger import logger


def transform_raw_table() -> None:
    with get_db_connection() as conn:
        cur = conn.cursor()
        source_table = "table_shops"
        target_table = "transformed_table"
        query = f"""
        INSERT INTO {target_table}
        SELECT 
           s.*,
            LOWER(TO_CHAR(TO_TIMESTAMP(s.dates, 'YYYY-MM-DD-HH'), 'FMDay')) AS day
        FROM {source_table} s;
        """
        try:
            cur.execute(query)
            conn.commit()
            logger.info(f"Successfully transformed table from {source_table} to {target_table}")
        except Exception as e:
            logger.error(f"Failed to transform table from {source_table} to {target_table}: {e}")

if __name__ == "__main__":
    transform_raw_table()