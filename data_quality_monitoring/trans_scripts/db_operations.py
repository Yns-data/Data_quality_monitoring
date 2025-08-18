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
    if not Path(csv_path).exists():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
    if table_name is None:
        table_name = Path(csv_path).stem

    if create_table:
        query = f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_csv_auto('{csv_path}')
        """
    else:
        query = f"""
            INSERT INTO {table_name}
            SELECT * FROM read_csv_auto('{csv_path}')
        """
    
    return query.strip(), create_table 

def get_insert_query_from_dataframe(
    df: pd.DataFrame,
    table_name: str,
) -> Tuple[str, bool]:
    """Generate SQL query to insert data from a pandas DataFrame into a DuckDB table.
    
    Args:
        df: pandas DataFrame containing the data to insert
        table_name: Name of the target table
        
    Returns:
        Tuple[str, bool]: (SQL query string, True if CREATE TABLE query, False if INSERT query)
        
    Raises:
        ValueError: If DataFrame is empty
    """
    if df.empty:
        raise ValueError("DataFrame cannot be empty")
    
    columns = list(df.columns)
    values_list = []
    for _, row in df.iterrows():
        values = []
        for val in row:
            if pd.isna(val):
                values.append('NULL')
            elif isinstance(val, str):
                values.append(f"'{val}'")
            else:
                values.append(str(val))
        values_list.append(f"({', '.join(values)})")
    else:
        query = f"""
            INSERT INTO {table_name} ({', '.join(columns)})
            VALUES {', '.join(values_list)}
        """
    
    return query.strip()