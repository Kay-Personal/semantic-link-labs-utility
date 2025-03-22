import pandas as pd
import urllib.parse
from uuid import UUID
import sempy.fabric as fabric
import sempy_labs._icons as icons
from typing import Optional, Callable, Dict
from sempy_labs.perf_lab._test_suite import TestSuite
from sempy_labs.perf_lab._test_cycle import refresh_test_models
from sempy_labs._helper_functions import _create_spark_session
from sempy_labs.lakehouse import get_lakehouse_tables
from sempy_labs.tom import connect_semantic_model
from sempy_labs._delta_analyzer import delta_analyzer, get_delta_table_history

FilterCallback = Callable[[str, str, dict], bool]

def _filter_by_prefix(
    table_name: str, 
    source_table_name: str, 
    filter_properties: Optional[dict] = None
) -> bool:
    """
    A sample implmentation of a FilterCallback function.
    Returns a boolean to indicate if the table info should be included in the source table list (True) or not (False).

    Parameters
    ----------
    table_name : str
        The name of the table in a semantic model. This sample function doesn't use this parameter, but it still needs to be present.
    source_table_name : str
        The name of the table in a data source.
    filter_properties: dict, default=None
        An arbirary dictionary of key/value pairs that the provision_perf_lab_lakehouse function passes to the table_generator function.
        The _filter_by_prefix sample function expects to find a 'Prefix' key in the filter_properties.

    Returns
    -------
    bool
        Indicates if the table should be included (True) or ignored (False).
    """

    if filter_properties is None:
        return False

    return source_table_name.startswith(
        filter_properties["Prefix"])


def get_source_tables(
    test_suite: TestSuite,
    filter_properties: Optional[dict] = None,
    filter_function: Optional[FilterCallback] = None
) -> 'pyspark.sql.DataFrame':
    """
    Returns a Spark dataframe with information about the source tables that the test semantic models use.

    Parameters
    ----------
    test_suite : TestSuite
        A TestSuite object with the test definitions.
    filter_properties: dict, default=None
        A dictionary of key/value pairs that the _get_source_tables() function passes to the filter_function function.
        The key/value pairs in the dictionary are specific to the filter_function function passed into the _get_source_tables() function.        
    filter_function
        A callback function to which source Delta tables to include in the dataframe returned to the caller.

    Returns
    -------
    pyspark.sql.DataFrame
        A PySpark dataframe containing the filtered source Delta tables.
        The returned dataframe includes the following columns:
        +----------+--------------------+--------------+---------------+----------------+-------------------+---------------+------------+----------+--------------+
        | ModelName|      ModelWorkspace|ModelTableName| DatasourceName|  DatasourceType|DatasourceWorkspace|SourceTableName|SourceFormat|SourceType|SourceLocation|
        +----------+--------------------+--------------+---------------+----------------+-------------------+---------------+------------+----------+--------------+
    """
    from pyspark.sql.types import StructType, StructField, StringType

    spark = _create_spark_session()

    # A table to return the source tables in a Spark dataframe.
    schema = StructType([
        StructField("ModelName", StringType(), nullable=False),
        StructField("ModelWorkspace", StringType(), nullable=False),
        StructField("ModelTableName", StringType(), nullable=False),
        StructField("DatasourceName", StringType(), nullable=False),
        StructField("DatasourceWorkspace", StringType(), nullable=False),
        StructField("DatasourceType", StringType(), nullable=False),
        StructField("SourceTableName", StringType(), nullable=False),
        StructField("SourceFormat", StringType(), nullable=False),
        StructField("SourceType", StringType(), nullable=False),
        StructField("SourceLocation", StringType(), nullable=False),
    ])
    rows = []

    for row in test_suite.to_df().dropDuplicates(['TargetWorkspace', 'TargetDataset','DatasourceName','DatasourceWorkspace','DatasourceType']).collect():
        target_dataset = row['TargetDataset']
        target_workspace = row['TargetWorkspace']
        data_source_name = row['DatasourceName']
        data_source_workspace = row['DatasourceWorkspace']
        data_source_type = row['DatasourceType']

        # Skip this row if the data_source_type is invalid.
        if not data_source_type == "Lakehouse":
            print(f"{icons.red_dot} Invalid data source type '{data_source_type}' detected. Ignoring this row. Please review your test definitions.")
            continue 

        # Skip this row if the target semantic model is not defined.
        if not target_dataset:
            print(f"{icons.red_dot} No test semantic model specifed as the target dataset. Ignoring this row. Please review your test definitions.")
            continue

        # Skip this row if the data_source_name is not defined.
        if not data_source_name:
            print(f"{icons.red_dot} No data source found for test semantic model '{target_dataset}'. Ignoring this row. Please review your test definitions.")
            continue

        # Skip this row if the target_workspace does not exist.
        filter_condition = urllib.parse.quote(target_workspace)
        dfW = fabric.list_workspaces(
            filter=f"name eq '{filter_condition}' or id eq '{filter_condition}'"
        )
        if dfW.empty:
            print(
                f"{icons.red_dot} Unable to resolve the target workspace '{target_workspace}' for test semantic model '{target_dataset}'. Ignoring this row. Please review your test definitions."
            )
            continue
        target_workspace_name = dfW.iloc[0]["Name"]
        target_workspace_id = dfW.iloc[0]["Id"]

        dfSM = fabric.list_datasets(workspace=target_workspace_name, mode="rest")
        dfSM = dfSM[
            (dfSM["Dataset Name"] == target_dataset)
            | (dfSM["Dataset Id"] == target_dataset)
        ]        
        if dfSM.empty:
            print(f"{icons.red_dot} Unable to find test semantic model '{target_dataset}'. Please review your test definitions and make sure all test semantic models are provisioned.")
            continue
        else:
            target_dataset_name = dfSM.iloc[0]["Dataset Name"]
            ltbls_df = get_lakehouse_tables(lakehouse=data_source_name, workspace=data_source_workspace)
            
            with connect_semantic_model(dataset=target_dataset_name, workspace=target_workspace_id, readonly=True) as tom:
                for t in tom.model.Tables:
                    for p in t.Partitions:
                        try:
                            table_name = t.get_Name()
                            source_table_name = p.Source.EntityName

                            st_df = ltbls_df[ltbls_df["Table Name"] == source_table_name]
                            if not st_df.empty:
                                if filter_function is None or filter_function(table_name, source_table_name, filter_properties) == True:
                                    
                                    # Get the first row for the source tables filtered by source table name.
                                    record = st_df.iloc

                                    rows.append((
                                        target_dataset_name,
                                        target_workspace_name,
                                        table_name,
                                        data_source_name,
                                        data_source_workspace,
                                        data_source_type,
                                        source_table_name,
                                        record[0]["Format"],
                                        record[0]["Type"],
                                        record[0]["Location"],
                                    ))                               
                            else:
                                print(f"{icons.red_dot} Delta table '{source_table_name}' not found in data source {data_source_type} '{data_source_name}' in workspace '{data_source_workspace}'.")
                        except:
                            continue                   

    return spark.createDataFrame(rows, schema=schema).dropDuplicates()


def get_storage_table_column_segments(
    test_suite: TestSuite,
    tables_info: 'pyspark.sql.DataFrame',
    refresh_type: str = "full",
) -> 'pyspark.sql.DataFrame':
    """
    Queries the INFO.STORAGETABLECOLUMNSEGMENTS DAX function for all model tables in the tables_info dataframe.

    Parameters
    ----------
    test_cycle_definitions : TestSuite
        A TestSuite object with test-cycle augmented test definitions, usually obtained by using the _initialize_test_cycle() function.
    tables_info : pyspark.sql.DataFrame
        A PySpark dataframe with information about the model tables and source tables, usually obtained by using the get_source_tables() function.
    refresh_type : str, Default = full
        The type of processing to perform for each test semantic model before gathering column segment data.
        Types align with the TMSL refresh command types: full, clearValues, calculate, dataOnly, automatic, and defragment.
        The add type isn't supported.
        In addition, refresh_type can be set to clearValuesFull, which performs a clearValues refresh followed by a full refresh.

    Returns
    -------
    pyspark.sql.DataFrame
        A PySpark dataframe containing the data retrieved from the INFO.STORAGETABLECOLUMNSEGMENTS DAX function.
    """
    from pyspark.sql.functions import lit

    spark = _create_spark_session()

    if refresh_type:
        refresh_test_models(
            test_suite = test_suite,
            refresh_type = refresh_type,
        )

    # Initialize an empty DataFrame
    results_df = None
    test_cycle_definitions = test_suite.to_df()
    for row in test_cycle_definitions.dropDuplicates(
        ["TargetWorkspace", "TargetDataset"]).collect():

        target_dataset = row["TargetDataset"]
        target_workspace = row["TargetWorkspace"]

        # Skip this row if the target semantic model is not defined.
        if not target_dataset:
            print(
                f"{icons.red_dot} The target dataset info is missing. Ignoring this row. Please review your test definitions."
            )
            continue

        # Skip this row if the target_workspace does not exist.
        filter_condition = urllib.parse.quote(target_workspace)
        dfW = fabric.list_workspaces(
            filter=f"name eq '{filter_condition}' or id eq '{filter_condition}'"
        )
        if dfW.empty:
            print(
                f"{icons.red_dot} Unable to resolve the target workspace '{target_workspace}' for test semantic model '{target_dataset}'. Ignoring this row. Please review your test definitions."
            )
            continue
        target_workspace_name = dfW.iloc[0]["Name"]
        target_workspace_id = dfW.iloc[0]["Id"]

        dfSM = fabric.list_datasets(workspace=target_workspace_name, mode="rest")
        dfSM = dfSM[
            (dfSM["Dataset Name"] == target_dataset)
            | (dfSM["Dataset Id"] == target_dataset)
        ]
        if dfSM.empty:
            print(
                f"{icons.red_dot} Unable to find test semantic model '{target_dataset}'. Please review your test definitions and make sure all test semantic models are provisioned."
            )
            continue
        else:
            target_dataset_name = dfSM.iloc[0]["Dataset Name"]
            target_dataset_id = dfSM.iloc[0]["Dataset Id"]

            # Format the dax query, but include only the tables that are interesting for the current model.
            model_table_rows = tables_info.where(
                (tables_info["ModelName"] == target_dataset)
                & (tables_info["ModelWorkspace"] == target_workspace)
            )
            dax_query = """
            EVALUATE FILTER(INFO.STORAGETABLECOLUMNSEGMENTS(), 
            """
            or_str = ""
            for t in model_table_rows.dropDuplicates(["ModelTableName"]).collect():
                dax_query += f"""{or_str} left([TABLE_ID], {len(t['ModelTableName']) + 2}) = \"{t['ModelTableName']} (\"
                """
                or_str = "||"

            dax_query += ")"

            dax_fdf = fabric.evaluate_dax(
                workspace=target_workspace_name,
                dataset=target_dataset_name,
                dax_string=dax_query,
            )

            # Add some more information from the test_cycle_definitions
            dax_df = spark.createDataFrame(dax_fdf)
            dax_df = (
                dax_df.withColumn("[WORKSPACE_NAME]", lit(target_workspace_id))
                .withColumn("[WORKSPACE_ID]", lit(target_dataset_name))
                .withColumn("[DATASET_NAME]", lit(target_dataset_name))
                .withColumn("[DATASET_ID]", lit(target_dataset_id))
                .withColumn("[TESTRUNID]", lit(row["TestRunId"]))
                .withColumn("[TESTRUNTIMESTAMP]", lit(row["TestRunTimestamp"]))
            )

            # Remove the [ ] brackets from the column names
            clean_columns = [col.replace("[", "").replace("]", "") for col in dax_df.columns]
            dax_df = dax_df.toDF(*clean_columns)

            # If results_df is None, initialize it with the first DataFrame
            if results_df is None:
                results_df = dax_df
            else:
                # Merge the DataFrame with the existing merged DataFrame
                results_df = results_df.union(dax_df)

    return results_df


def analyze_delta_tables(
    tables_info: 'pyspark.sql.DataFrame',
    approx_distinct_count: bool = True,
    column_stats: bool = True,
    skip_cardinality: bool = True,
) -> Dict[str, Dict[str, pd.DataFrame]]:
    """
    Analyzes a delta table and returns the results in a dictionary of dictionaries containing a set of 6 dataframes for each table.

    The 6 dataframes returned by this function are:

    * Summary
    * Parquet Files
    * Row Groups
    * Column Chunks
    * Columns
    * History

    Read more about Delta Analyzer `here <https://github.com/microsoft/Analysis-Services/tree/master/DeltaAnalyzer>`_.

    Parameters
    ----------
    tables_info : pyspark.sql.DataFrame
        A PySpark dataframe with information about the model tables and source tables, usually obtained by using the get_source_tables() function.
    approx_distinct_count: bool, default=True
        If True, uses approx_count_distinct to calculate the cardinality of each column. If False, uses COUNT(DISTINCT) instead.
    column_stats : bool, default=True
        If True, collects data about column chunks and columns. If False, skips that step and only returns the other 3 dataframes.
    skip_cardinality : bool, default=True
        If True, skips the cardinality calculation for each column. If False, calculates the cardinality for each column.

    Returns
    -------
    Dict[str, Dict[str, pandas.DataFrame]]
        A dictionary of dictionaries containing pandas dataframes with the Delta Analyzer results for each table.
    """

    results: Dict[str, Dict[str, pd.DataFrame]] = {}

    delta_tables = tables_info.where(tables_info["DatasourceType"] == 'Lakehouse').dropDuplicates(['DatasourceWorkspace', 'SourceTableName', 'DatasourceName'])
    for table in delta_tables.collect():
        table_name = table['SourceTableName']
        lakehouse_name = table['DatasourceName']
        workspace_name = table['DatasourceWorkspace']

        print(f"{icons.green_dot} Analyzing Delta table '{table['SourceTableName']}' in Lakehouse '{table['DatasourceName']}'.")

        da_results = delta_analyzer(
            table_name = table_name,
            lakehouse = lakehouse_name,
            workspace = workspace_name,
            approx_distinct_count = approx_distinct_count,
            column_stats = column_stats,
            skip_cardinality = skip_cardinality,
        )

        da_results['History'] = get_delta_table_history(
            table_name = table_name,
            lakehouse = lakehouse_name,
            workspace = workspace_name,
        )
        
        results[table_name] = da_results
    
    return results