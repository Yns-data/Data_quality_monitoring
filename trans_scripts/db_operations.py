from pathlib import Path
from typing import Optional, Tuple
import pandas as pd


def get_insert_query(
    csv_path: str,
    table_name: Optional[str] = None,
    create_table: bool = False
) -> Tuple[str, bool]:
    """Generate SQL query to insert data from a CSV file into a DuckDB table.
    
    Args:
        csv_path: Path to the CSV file to insert
        table_name: Name of the target table (defaults to CSV filename without extension)
        create_table: Whether to generate CREATE TABLE query instead of INSERT
        
    Returns:
        Tuple[str, bool]: (SQL query string, True if CREATE TABLE query, False if INSERT query)
        
    Raises:
        FileNotFoundError: If CSV file doesn't exist
    """
    # Validate CSV path
    if not Path(csv_path).exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
    # Default table name to CSV filename if not provided
    if table_name is None:
        table_name = Path(csv_path).stem

    if create_table:
        # Generate CREATE TABLE query
        query = f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_csv_auto('{csv_path}')
        """
    else:
        # Generate INSERT query
        query = f"""
            INSERT INTO {table_name}
            SELECT * FROM read_csv_auto('{csv_path}')
        """
    
    return query.strip(), create_table 

def get_insert_query_from_dataframe(
    df: pd.DataFrame,
    table_name: str,
    create_table: bool = False
) -> Tuple[str, bool]:
    """Generate SQL query to insert data from a pandas DataFrame into a DuckDB table.
    
    Args:
        df: pandas DataFrame containing the data to insert
        table_name: Name of the target table
        create_table: Whether to generate CREATE TABLE query instead of INSERT
        
    Returns:
        Tuple[str, bool]: (SQL query string, True if CREATE TABLE query, False if INSERT query)
        
    Raises:
        ValueError: If DataFrame is empty
    """
    if df.empty:
        raise ValueError("DataFrame cannot be empty")
    
    # Get columns from DataFrame
    columns = list(df.columns)
    
    # Prepare values
    values_list = []
    for _, row in df.iterrows():
        values = []
        for val in row:
            if pd.isna(val):  # Handle NaN and None values
                values.append('NULL')
            elif isinstance(val, str):
                values.append(f"'{val}'")
            else:
                values.append(str(val))
        values_list.append(f"({', '.join(values)})")
    
    if create_table:
        # Generate CREATE TABLE query with INSERT
        query = f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM (VALUES {', '.join(values_list)}) 
            AS t({', '.join(columns)})
        """
    else:
        # Generate INSERT query
        query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES {', '.join(values_list)}
        """
    
    return query.strip(), create_table