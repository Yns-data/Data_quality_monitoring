# type: ignore
from data_quality_monitoring.conn_scripts.db_connection import get_db_connection
from data_quality_monitoring.conn_scripts.project_logger import logger
import pandas as pd
import os


from data_quality_monitoring.trans_scripts.get_sql_queries import (
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
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = os.path.join(script_dir, "calculation_folder")
    if os.path.exists(output_dir):
        logger.info(f"{output_dir} exists")    
    else:
        os.mkdir(output_dir)
        logger.info(f"{output_dir} has been created")
    output_path = os.path.join(output_dir,"prc_dataframe.csv")
    perct_dataframe.to_csv(output_path, sep=";", index=False)
except Exception as e:
    logger.info(f"An exception has occured while executing query : {e}")


