# type: ignore
from typing import Optional
TABLE_NAME = "table_shops"

def get_basic_clause(
    columns_to_transform: list[str],
    op: Optional[str | list[str]],
    valid_sql_op: set[str] = {
        "SUM", "AVG", "MIN", "MAX", "STDDEV", "MEDIAN", "COUNT", "VAR",
        "FIRST", "LAST", "MODE", "PERCENTILE_25", "PERCENTILE_75", "PERCENTILE_90"
    }
) -> str:
    """
    Generate a SQL SELECT clause for aggregating columns with specified operations.

    Args:
        columns_to_transform (list[str]): List of column names to aggregate.
        op (str | list[str]): SQL aggregation operation(s), either a single string or a list of strings.
        valid_sql_op (set[str], optional): Set of valid SQL aggregation operations.

    Returns:
        str: SQL SELECT clause containing aggregation expressions for each column and operation.

    Raises:
        ValueError: If operation is not in valid_sql_op set, or if too many operations specified.
        TypeError: If op is not a string or list of strings.
    """
    if isinstance(op, str):
        if op not in valid_sql_op:
            raise ValueError(f"Operation '{op}' is not valid. Please choose from: {valid_sql_op}")
    elif isinstance(op, list):
        if not all(isinstance(o, str) for o in op):
            raise TypeError("All operations must be strings")
        if len(op) > len(valid_sql_op):
            raise ValueError(f"Maximum {len(valid_sql_op)} operations allowed")
        invalid_ops = set(op) - valid_sql_op
        if invalid_ops:
            raise ValueError(f"Invalid operations: {invalid_ops}. Must be from: {valid_sql_op}")
    else:
        raise TypeError("Operation must be string or list of strings")

    if isinstance(op, str):
        calculation_expressions = [f"{op}({col}) AS {op.lower()}_{col}" for col in columns_to_transform]
    else:
        calculation_expressions = [
            f"{o}({col}) AS {o.lower()}_{col}"
            for o in op
            for col in columns_to_transform
        ]
    select_clause = ",\n               ".join(calculation_expressions)
    return select_clause

def get_prcentage_clause(columns_to_transform: list[str], op="SUM") -> str:
    """
    Generate a SQL clause for calculating percentages of columns relative to their total.

    Args:
        columns_to_transform (list[str]): List of column names to calculate percentages for.
        op (str, optional): SQL aggregation operation (defaults to "SUM").

    Returns:
        str: SQL clause with basic aggregations and percentage calculations.
    """
    excluded_cols = {"visitors", "pages_viewed"}
    columns_to_pourcntage = [col for col in columns_to_transform if col not in excluded_cols]
    basic_clause = get_basic_clause(columns_to_pourcntage, op)
    op_lower = op.lower()
    total_expr = " + ".join(f"{op_lower}_{col}" for col in columns_to_pourcntage)
    prc_expr = ",\n               ".join(
        f"{op_lower}_{col}/total AS prc_{col}"
        for col in columns_to_pourcntage
    )
    return f"{basic_clause},\n               {total_expr} AS total,\n               {prc_expr}"

def get_basic_aggregate_query(
    columns_to_transform: list[str],
    select_clause: str,
    agg_parameter: Optional[str | list[str]],
    valid_parameters: set[str] = {"hour", "day", "week", "month", "year", "cities"}
) -> str:
    """
    Generate a SQL query for aggregating data by time period or city.

    Args:
        columns_to_transform (list[str]): List of column names to aggregate.
        select_clause (str): SQL SELECT clause with aggregations.
        agg_parameter (str | list[str]): Column(s) to group by, either a single string or a list of up to 2 strings.
        valid_parameters (set[str], optional): Set of valid parameters to group by.

    Returns:
        str: SQL query string that aggregates the specified columns by the given parameters.

    Raises:
        TypeError: If agg_parameter is not a string or list of max 2 strings.
        ValueError: If any parameter is not in valid_parameters.
    """
    if isinstance(agg_parameter, str):
        agg_parameters = [agg_parameter]
    elif isinstance(agg_parameter, list) and len(agg_parameter) <= 2:
        agg_parameters = agg_parameter
    else:
        raise TypeError("agg_parameter must be string or list of max 2 strings")

    invalid_params = set(agg_parameters) - valid_parameters
    if invalid_params:
        raise ValueError(f"Invalid parameter(s): {invalid_params}. Must be from: {valid_parameters}")

    group_by = ", ".join(agg_parameters)
    subquery = f"""
        SELECT 
            *,
            RIGHT(dates, 2) AS heure,
            LEFT(dates, 10) AS day,
            TO_DATE(LEFT(dates, 10), 'YYYY-MM-DD') AS date_val,
            TO_CHAR(TO_DATE(LEFT(dates, 10), 'YYYY-MM-DD'), 'IYYY-IW') AS week,
            TO_CHAR(TO_DATE(LEFT(dates, 10), 'YYYY-MM-DD'), 'YYYY-MM') AS month, 
            TO_CHAR(TO_DATE(LEFT(dates, 10), 'YYYY-MM-DD'), 'YYYY') AS year
        FROM 
            {TABLE_NAME}
        WHERE 
            GREATEST({', '.join(columns_to_transform)}) IS NOT NULL"""

    return f"""
        SELECT 
            {group_by},
            {select_clause}
        FROM 
            ({subquery})
        GROUP BY 
            {group_by}
        ORDER BY 
            {group_by}"""

def add_day_condition(query: str, day: str) -> str:
    """
    Add a condition to filter the SQL query by a specific day of the week.

    Args:
        query (str): SQL query string to modify.
        day (str): Day name (sunday through saturday).

    Returns:
        str: Modified query with day filter added.

    Raises:
        ValueError: If the day is not valid.
    """
    day_to_number = {
        "sunday": '0',
        "monday": '1',
        "tuesday": '2',
        "wednesday": '3',
        "thursday": '4',
        "friday": '5',
        "saturday": '6'
    }
    if day.lower() not in day_to_number:
        raise ValueError(f"Invalid day. Must be one of: {', '.join(day_to_number.keys())}")
    return query.replace(
        "GROUP BY",
        f"WHERE EXTRACT(DOW FROM date_val) = {day_to_number[day.lower()]}\n        GROUP BY"
    )

def get_prc_query(
    columns_to_transform: list[str],
    agg_parameter: Optional[str | list[str]],
    )->str:
    sum_select_clause = get_basic_clause(op="SUM",columns_to_transform=columns_to_transform)
    sum_aggregation= get_basic_aggregate_query(
        select_clause=sum_select_clause,
        columns_to_transform=columns_to_transform,
        agg_parameter=agg_parameter)

    total = " + ".join([f"sum_{col}"for col in columns_to_transform])
    prc_exp = "\n           ,".join([f"ROUND(sum_{col}::numeric/NULLIF({total},0)::numeric,2)*100 AS prct_{col}" for col in columns_to_transform ])
    return f"""
            SELECT *,
            {prc_exp},
            ROUND(({total})::numeric,2) AS Total
            FROM(
            {sum_aggregation}
            )
            """
