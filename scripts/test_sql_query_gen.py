
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parents[2]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from Data_quality_monitoring.conn_scripts.db_connection import get_db_connection
from Data_quality_monitoring.conn_scripts.project_logger import logger
import pandas as pd


from Data_quality_monitoring.trans_scripts.get_sql_queries import (
    get_prc_query,
    add_day_condition,
)

col_to_transform=["visitors", 
 "pages_viewed", 
 "food_articles", 
 "wear_articles", 
 "electronics_articles", 
 "sports_articles", 
 "toys_articles", 
 "home_articles", 
 "garden_articles", 
 "beauty_articles", 
 "automotive_articles"]

day_agreggate = "monday"
agreggate_param = "day"

try :
    prct_query =get_prc_query(agg_parameter=agreggate_param,columns_to_transform=col_to_transform)
    logger.info("Percentage query created")

    if day_agreggate:
        final_query = add_day_condition(query=prct_query,day=day_agreggate)
        logger.info("Day condition has been added")
    else:
        final_query = prct_query

    with get_db_connection() as conn:
        perct_dataframe = pd.read_sql(final_query,conn)

    perct_dataframe.to_csv("calculation_folder/perct_dataframe.csv", sep=";", index=False)
except Exception as e:
    logger.info(f"An exception has occured while executing query : {e}")


