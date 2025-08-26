# type: ignore
from data_quality_monitoring.conn_scripts.db_connection import get_db_connection
from data_quality_monitoring.conn_scripts.project_logger import logger
import pandas as pd
import os
from typing import List


from data_quality_monitoring.trans_scripts.get_sql_queries import (
    get_prc_query,
)

# Configuration
COL_TO_TRANSFORM = [
    "visitors", "pages_viewed", "food_articles", "wear_articles", 
    "electronics_articles", "sports_articles", "toys_articles", 
    "home_articles", "garden_articles", "beauty_articles", "automotive_articles"
]

DAYS_OF_WEEK = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
AGGREGATE_PARAMETERS = ["day", "week", "year"]


def create_output_directory(script_dir: str) -> str:
    """Create and return the output directory path."""
    output_dir = os.path.join(script_dir, "calculation_folder")
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"{output_dir} has been created")
    else:
        logger.info(f"{output_dir} exists")
    return output_dir

def process_query_without_combination(agg_param: str, columns: List[str], output_dir: str) -> bool:
    try:
        prct_query = get_prc_query(agg_parameter=agg_param, columns_to_transform=columns)
        logger.info(f"Percentage query created for {agg_param}")
        with get_db_connection() as conn:
            perct_dataframe = pd.read_sql(prct_query, conn)      
        output_path = os.path.join(output_dir, f"percent_calculated_each_{agg_param}_dataframe.csv")
        perct_dataframe.to_csv(output_path, sep=";", index=False)
        logger.info(f"Successfully saved results to {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to process query with aggregation '{agg_param}': {e}")
        return False

def process_query_combination(agg_param: str, columns: List[str], output_dir: str) -> bool:
    """Process a single query combination and save results."""
    try:
        # Generate base query
        prct_query = get_prc_query(agg_parameter=agg_param, columns_to_transform=columns)
        logger.info(f"Percentage query created for {agg_param}")

        # Execute query and save results
        with get_db_connection() as conn:
            perct_dataframe = pd.read_sql(prct_query, conn)
        
        output_path = os.path.join(output_dir, f"percent_calculated_each_{agg_param}_dataframe.csv")
        perct_dataframe.to_csv(output_path, sep=";", index=False)
        logger.info(f"Successfully saved results to {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to process query with aggregation '{agg_param}': {e}")
        return False


def main():
    """Main execution function with optimized error handling and structure."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_dir = create_output_directory(script_dir)
    
    success_count = 0
    total_combinations = len(AGGREGATE_PARAMETERS) * len(DAYS_OF_WEEK)
    
    for agg_param in AGGREGATE_PARAMETERS:
        logger.info(f"Processing aggregation parameter: {agg_param}")
        if process_query_combination(agg_param, COL_TO_TRANSFORM, output_dir):
                success_count += 1
    
    logger.info(f"Processing complete: {success_count}/{total_combinations} combinations successful")


if __name__ == "__main__":
    main()