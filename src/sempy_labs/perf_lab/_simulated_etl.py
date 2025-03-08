from typing import Optional, Tuple, Callable, List
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    _create_spark_session,
    _is_valid_date,
    is_valid_integer
    )

UpdateTableCallback = Callable[['pyspark.sql.Row', dict], None]

def simulate_etl(
    source_tables_info: 'pyspark.sql.DataFrame',
    update_properties: Optional[dict] = None,
    update_function: Optional[UpdateTableCallback] = None
):
    """
    Simulates an ETL process by rolling the tables in the tables_df forward by using the specified update_function.

    Parameters
    ----------
    source_tables_info : pyspark.sql.DataFrame
        A PySpark dataframe with information about the model tables and source tables, usually obtained by using the get_source_tables() function.
    update_properties : dict, default=None
        A dictionary of key/value pairs that the simulate_etl() function passes to the update_function callback.
        The key/value pairs in the dictionary are specific to the update_function implementation.
    update_function : UpdateTableCallback, default=None
        A callback function to process each source table.
    """
    if update_function:
        for row in source_tables_info.dropDuplicates(["SourceLocation", "DatasourceName", "DatasourceType", "DatasourceWorkspace", "SourceTableName"]).collect():
            update_function(row, update_properties)
    else:
            raise ValueError("Unable to process tables without an UpdateTableCallback. Please set the update_function parameter.")
    
    print(f"{icons.green_dot} ETL processing completed.")


def delete_reinsert_rows(
    source_table_info: 'pyspark.sql.Row',
    custom_properties: Optional[dict] = None
) -> None:
    """
    Deletes and reinserts rows in a Delta table and optimizes the Delta table between deletes and inserts.
    Parameters
    ----------
    source_table_info: pyspark.sql.Row
        A PySpark row with the following columns:
        +---------------+----------------+-------------------+---------------+--------------+
        | DatasourceName|  DatasourceType|DatasourceWorkspace|SourceTableName|SourceLocation|
        +---------------+----------------+-------------------+---------------+--------------+
     custom_properties: dict, default=None
        A dictionary of key/value pairs specific to the callback function.
    """
    from datetime import datetime, timedelta

    key_column = custom_properties["key_column"]
    optimize_table = custom_properties["Optimize"]

    minmax = _get_min_max_keys(
        source_table_info['SourceTableName'], 
        source_table_info['SourceLocation'], 
        key_column)

    if minmax:
        if _is_valid_date(str(minmax[1])):
            # Calculate the date id for the next (max+1) day for the new rows.
            new_date = datetime.strptime(str(minmax[1]), "%Y%m%d") + timedelta(days=1)     
            new_value = int(new_date.strftime("%Y%m%d"))
        elif is_valid_integer(minmax[1]):
            new_value = int(minmax[1]) + 1
        else:
            new_value = minmax[1]

        _sliding_window_update(
                table_name=source_table_info['SourceTableName'],
                table_path=source_table_info['SourceLocation'],
                old_value= minmax[0],
                new_value= new_value,
                key_column=key_column,
                compress = optimize_table,
                )    

    return None


def _sliding_window_update(
    table_name: str,
    table_path: str,
    old_value: object,
    new_value: object,
    key_column: str,
    compress: Optional[bool] = True,
) -> 'pyspark.sql.DataFrame':
    """
    Deletes the rows in a Delta table that match the old value in the specified column,
    then optionally compats the Delta table, and then reinserts the old rows with the old value replaced by new value.
    This is very similar to a normal update operation, with the exception that 
    this method optionally optimizes the Delta table between deletes and inserts.
    Parameters
    ----------
    table_name : str
        The name if a Delta table from which to delete the rows.
    table_path : str
        The full path to the Delta table to read its data.
    old_value : object
        The old value to match in the specified column.
    new_value : object
        The new value in the specified column for the matching rows.
    key_column : str
        The name of the column to filter the rows to delete and insert.
    compress : bool, Default = True
        A flag that indicates if the Delta table should be optimized between deletes and inserts.
    Returns
    -------
    pyspark.sql.DataFrame
        A PySpark dataFrame with the reinserted rows.
    """ 
    from delta.tables import DeltaTable
    from pyspark.sql.functions import lit

    deleted_rows = _delete_rows(
        table_name,
        table_path,
        old_value,
        key_column,
        )
    
    if compress:
        # SPARK optimize the table
        spark = _create_spark_session()
        delta_table = DeltaTable.forPath(spark, table_path)
        delta_table.optimize().executeCompaction()
        print(f"{icons.checked} Delta table {table_name} optimized.")

    # Detect the datatype of the key_column
    column_type = dict(deleted_rows.dtypes)[key_column]
    updated_rows = deleted_rows.withColumn(key_column, lit(new_value).cast(column_type))
    _insert_rows(
        updated_rows,
        table_name,
        table_path,
        )

    return updated_rows


def _delete_rows(
    table_name: str,
    table_path: str,
    key_value: str,
    key_column: Optional[str] = "DIM_DateId"
) -> 'pyspark.sql.DataFrame':
    """
    Deletes and returns the rows from a Delta table that have a given value in a given column.

    Parameters
    ----------
    table_name : str
        The name if a Delta table from which to delete the rows.
    table_path : str
        The full path to the Delta table to read its data.
    key_value : str
        The filter value for which to filter and delete the rows.
    key_column : str, Default = "DIM_DateId"
        The name of the column for which to filter and delete the rows.

    Returns
    -------
    pyspark.sql.DataFrame
        A PySpark DataFrame with the deleted rows.
    """    
    spark = _create_spark_session()
    table_df = spark.read.format("delta").load(table_path)

    rows_df = table_df[table_df[key_column] == key_value]
    rows_df = spark.createDataFrame(rows_df.rdd, schema=rows_df.schema)

    table_df.createOrReplaceTempView(f"{table_name}_view")
    strDelete = f"DELETE From {table_name}_view WHERE {key_column} = '{key_value}'"
    spark.sql(strDelete)
    print(f"{icons.checked} {rows_df.count()} rows with {key_column} = '{key_value}' removed from table '{table_name}'.")

    return rows_df


def _insert_rows(
        data_df: 'pyspark.sql.DataFrame',
        table_name: str,
        table_path: str,
) -> None:
    """
    Inserts the rows from a Spark Dataframe into the specified Delta table.

    Parameters
    ----------
    data_df : pyspark.sql.DataFrame
        A PySpark dataframe with the data to be inserted into the specified Delta table.
    table_name : str
        The name if a Delta table from which to delete the rows.
    table_path : str
        The full path to the Delta table to read its data.

     """
    data_df.write.format("delta").mode("append").save(table_path)
    print(f"{icons.checked} {data_df.count()} rows inserted into table {table_name}, path '{table_path}'.")


def _update_rows(
    table_name: str,
    table_path: str,
    old_value: str,
    new_value: str,
    column_name: str
) -> 'pyspark.sql.DataFrame':
    """
    Updates the rows in a Delta table that match the old value in the specified column.

    Parameters
    ----------
    table_name : str
        The name if a Delta table from which to delete the rows.
    table_path : str
        The full path to the Delta table to read its data.
    old_value : str
        The old value to match in the specified column.
    new_value : str
        The new value in the specified column for the matching rows.
    column_name : str
        The name of the column to filter the rows and update the value.

    Returns
    -------
    pyspark.sql.DataFrame
        A PySpark DataFrame with the updated rows.
    """ 
    from pyspark.sql.functions import col

    return _update_delta_table(
        table_name,
        table_path,
        col(column_name) == old_value,
        new_value,
        column_name,
        )


def _update_delta_table(
    table_name: str,
    table_path: str,
    condition: 'pyspark.sql.functions.Column',
    new_value: str,
    column_name: str
) -> 'pyspark.sql.DataFrame':
    """
    Updates the rows from a Delta table that match the specified condition expression.

    Parameters
    ----------
    table_name : str
        The name if a Delta table from which to delete the rows.
    table_path : str
        The full path to the Delta table to read its data.
    condition : pyspark.sql.functions.Column
        The column expression to determine which rows to update in the Delta table, using various functions like col, when, lit, and others from the pyspark.sql.functions module.
    new_value : str
        The new value in the specified column for the matching rows.
    column_name : str
        The name of the column to update.

    Returns
    -------
    pyspark.sql.DataFrame
        A PySpark DataFrame with the updated rows.
    """  
    from delta.tables import DeltaTable
    from pyspark.sql.functions import when, col

    spark = _create_spark_session()

    # Load the Delta table
    delta_table = DeltaTable.forPath(spark, table_path)

    # Update the column based on the condition
    delta_table.update(
        condition,
        {column_name: when(condition, new_value).otherwise(col(column_name))}
    )

    # Load the updated rows as a DataFrame
    rows_df = spark.read.format("delta").load(table_path) \
        .filter(col(column_name) == new_value)

    print(f"{icons.checked} {rows_df.count()} rows updated in table {table_name}.")

    return rows_df


def _get_min_max_keys(
    table_name: str,
    table_path: str,
    key_column: Optional[str] = "DIM_DateId"
) -> Tuple[object, object]:
    """
    Gets the min and max values for a specified column from a Spark dataframe.

    Parameters
    ----------
    table_name : str
        The name if a Delta table from which to delete the rows.
    table_path : str
        The full path to the Delta table to read its data.
    key_column : str
        The name of column for which to return the min and max values.

    Returns
    -------
    Tuple[object,object]
        A tuple of the min and max values for the specified column.
    """
    from pyspark.sql.functions import min, max

    spark = _create_spark_session()
    table_df = spark.read.format("delta").load(table_path)

    # Get the min and max value of any data type from the specified table
    if key_column in table_df.columns:
        minKey = table_df.agg(min(key_column)).collect()[0][0]
        maxKey = table_df.agg(max(key_column)).collect()[0][0]
        return (minKey,maxKey)

    print(f"{icons.red_dot} The specified key column '{key_column}' was not found in {table_df.columns} of Delta table '{table_name}', path '{table_path}'.\r\nNote that column names are case senstive.")
    return None