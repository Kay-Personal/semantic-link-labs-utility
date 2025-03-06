
from typing import Optional, Tuple, List
from sempy_labs.perf_lab._test_suite import (
    TestSuite
)

import sempy.fabric as fabric
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    resolve_dataset_name_and_id,
    generate_guid,
    _get_or_create_workspace
)
from sempy_labs import deploy_semantic_model, clear_cache
from sempy_labs._refresh_semantic_model import refresh_semantic_model

def run_test_cycle(
    test_suite: TestSuite,
    clear_query_cache: Optional[bool] = True,
    refresh_type: Optional[str] = None,
    trace_timeout: Optional[int] = 60,
    tag: Optional[str] = None,
) -> Tuple['pyspark.sql.DataFrame', dict]:
    """
    Runs each DAX query and returns the traces events, such as QueryBegin and QueryEnd, together with the query results.

    Parameters
    ----------
    test_suite : TestSuite
        A TestSuite object with test-cycle augmented test definitions, usually obtained by using the _initialize_test_cycle() function.
    clear_query_cache : bool, Default = True
        Clear the query cache before running each query.
    refresh_type : str, Default = None
        The type of processing to perform for each test semantic model.
        Types align with the TMSL refresh command types: full, clearValues, calculate, dataOnly, automatic, and defragment. 
        The add type isn't supported.
        In addition, refresh_type can be set to clearValuesFull, which performs a clearValues refresh followed by a full refresh.
   trace_timeout : int, default=60
        The max time duration to capture trace events after execution of the DAX queries against a test semantic model.
        The trace_timeout applies on a model-by-model basis. If the test_cycle_definitions use x models,
        the timeout is applied x times, because each test model is traced individually.
    tag : str, default=None
        A string to provide additional information about a particular test cycle, such as cold, incremental, warm, or any other meaningful tag.
        This string is added in a 'Tag' column to all trace events.

    Returns
    -------
    Tuple[pyspark.sql.DataFrame, dict]
        A Spark dataframe with the trace events for each DAX query in the dictionary.
        A dictionary of FabricDataFrames with the query results for each DAX query.
    """
    import urllib.parse
    from pyspark.sql.functions import lit

    cycle_results_tuple = (None, {})
    test_cycle_definitions_df = test_suite.to_df()

    for row in test_cycle_definitions_df.dropDuplicates(['TargetWorkspace', 'TargetDataset', 'TestRunId', 'TestRunTimestamp']).collect():
        target_dataset = row['TargetDataset']
        target_workspace = row['TargetWorkspace']
        test_run_id = row['TestRunId']
        test_run_timestamp = row['TestRunTimestamp']

        # Skip this row if the target semantic model is not specified.
        if not target_dataset:
            print(f"{icons.red_dot} The target dataset info is missing. Ignoring this row. Please review your test definitions.")
            continue

        # Skip this row if the target_workspace does not exist.
        filter_condition = urllib.parse.quote(target_workspace)
        dfW = fabric.list_workspaces(filter=f"name eq '{filter_condition}' or id eq '{filter_condition}'")
        if dfW.empty:
            print(f"{icons.red_dot} Unable to resolve the target workspace '{target_workspace}' for test semantic model '{target_dataset}'. Ignoring semantic model '{target_dataset}'. Please review your test definitions.")
            continue 
        target_workspace_name = dfW.iloc[0]['Name']
        target_workspace_id = dfW.iloc[0]['Id']


        # Skip this row if the target_dataset does not exist.
        dfSM = fabric.list_datasets(target_workspace_id)
        dfSM = dfSM[(dfSM["Dataset Name"] == target_dataset) | (dfSM["Dataset ID"] == target_dataset)]
        if dfSM.empty:
            print(f"{icons.red_dot} Unable to find semantic model '{target_dataset}' in workspace '{target_workspace}'. Ignoring this semantic model. Please review your test definitions.")
            continue 
        target_dataset_name = dfSM.iloc[0]['Dataset Name']
        target_dataset_id = dfSM.iloc[0]['Dataset ID']


        # Prior to tracing the DAX queries for this model, perform the requested refreshes. 
        if refresh_type and refresh_type != "clearValuesFull":
            # The refresh type is supported by the refresh_semantic_model function.
            refresh_semantic_model( 
                dataset=target_dataset_id, 
                workspace=target_workspace_id, 
                refresh_type=refresh_type)
        elif refresh_type == "clearValuesFull":
            # The refresh type 'clearValuesFull' requires 2 refresh_semantic_model calls
            # 1. clearValues, 2. full
            refresh_semantic_model( 
                dataset=target_dataset_id, 
                workspace=target_workspace_id, 
                refresh_type="clearValues")
            refresh_semantic_model( 
                dataset=target_dataset_id, 
                workspace=target_workspace_id, 
                refresh_type="full")

        # Run all queries that use the current target semantic model,
        # and then merge the results with the overall cycle results.
        queries_df = test_cycle_definitions_df.filter(
            (test_cycle_definitions_df.TargetWorkspace == target_workspace) \
            & (test_cycle_definitions_df.TargetDataset == target_dataset))

        (trace_df, query_results) = _trace_dax_queries(
            dataset=target_dataset_name,
            dax_queries=_queries_toDict(queries_df),
            workspace = target_workspace_name,
            clear_query_cache=clear_query_cache,
        )

        if trace_df:
            # Add test cycle info to the trace events.
            trace_df = trace_df \
                .withColumn("TestRunId", lit(test_run_id)) \
                .withColumn("TestRunTimestamp", lit(test_run_timestamp)) \
                .withColumn("DatasetId", lit(target_dataset_id)) \
                .withColumn("WorkspaceId", lit(target_workspace_id)) \
                .withColumn("Tag", lit(tag)) \
                       
            if cycle_results_tuple[0]:
                cycle_results_tuple = (cycle_results_tuple[0].union(trace_df), cycle_results_tuple[1])
            else:
                cycle_results_tuple = (trace_df, cycle_results_tuple[1])

        if query_results:
            cycle_results_tuple = (cycle_results_tuple[0], cycle_results_tuple[1] | query_results)

    return cycle_results_tuple


def _queries_toDict(
    test_definitions: 'pyspark.sql.DataFrame'
) -> dict:
    """
    Returns the QueryId and QueryText columns from a test definitons dataframe in a dictionary.

    Parameters
    ----------
    test_definitions : pyspark.sql.DataFrame
        A PySpark dataframe with QueryId and QueryText columns.

    Returns
    -------
    dict
        A dictionary with the QueryId and QueryText columns.
    """
   
    rows = test_definitions.select(test_definitions.columns[:2]).collect()
    return {row: row for row in rows}


def _trace_dax_queries(
    dataset: str,
    dax_queries: dict,
    clear_query_cache: Optional[bool] = True,
    timeout: Optional[int] = 60,
    workspace: Optional[str] = None,
) -> Tuple['pyspark.sql.DataFrame', dict]:
    """
    Runs each DAX query specified in the dax_queries dictionary and returns trace events together with the query results.

    Parameters
    ----------
    dataset : str
        The mame of the semantic model.
    dax_queries : dict
        A dictionary of dax queries to run and trace, such as:
        {
            "Sales Amount Test", 'EVALUATE SUMMARIZECOLUMNS("Sales Amount", [Sales Amount])',
            "Order Quantity with Product", """ """EVALUATE SUMMARIZECOLUMNS('Product'[Color], "Order Qty", [Order Qty])""" """,
        }
    clear_query_cache : bool, Default=True
        Clear the query cache before running each query.
    timeout : int, default=60
        The max time duration to capture trace events after execution of the DAX queries.
    workspace : str, default=None
        The Fabric workspace name.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.

    Returns
    -------
    Tuple[pyspark.sql.DataFrame, dict]
        A Spark dataframe with the trace events for each DAX query in the dictionary.
        A dictionary of FabricDataFrames with the query results for each DAX query.
    """
    import warnings
    import time
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    warnings.filterwarnings("ignore", category=UserWarning)

    base_cols = ["EventClass", "EventSubclass", "CurrentTime", "NTUserName", "TextData", "ActivityID", "RequestID"]
    begin_cols = base_cols + ["StartTime"]
    end_cols = base_cols + ["StartTime", "EndTime", "Duration", "CpuTime", "Success"]

    event_schema = {
        "QueryBegin": begin_cols,
        "QueryEnd": end_cols,
        "VertiPaqSEQueryBegin": begin_cols,
        "VertiPaqSEQueryEnd": end_cols,
        "VertiPaqSEQueryCacheMatch": base_cols,
        "DirectQueryEnd": ["EventClass", "TextData", "Duration", "StartTime", "EndTime", "ActivityID", "RequestID"],
        "ExecutionMetrics": ["EventClass", "ApplicationName", "TextData", "ActivityID", "RequestID"],
    }

    query_results = {}
    last_executed_query = ""

    # Establish trace connection
    with fabric.create_trace_connection(dataset=dataset, workspace=workspace) as trace_connection:
        with trace_connection.create_trace(event_schema) as trace:
            trace.start()
            try:
                print(f"{icons.in_progress} Tracing {len(dax_queries)} dax queries...")
                
                # Tag and loop through the DAX queries
                tagged_dax_queries = _tag_dax_queries(dax_queries)
                for query in tagged_dax_queries:
                    query_name = query['QueryId']
                    query_text = query['QueryText']

                    if clear_query_cache:
                        clear_cache(dataset=dataset, workspace=workspace)

                    fabric.evaluate_dax(
                        dataset=dataset, workspace=workspace, dax_string="EVALUATE {1}")

                    result = fabric.evaluate_dax(
                        dataset=dataset, workspace=workspace, dax_string=query_text)

                    # Add results to output
                    query_results[query_name] = result
                    print(f"{icons.green_dot} The '{query_name}' query has completed with {result.shape[0]} rows.")
                    last_executed_query = query_name

            except Exception as e:
                print(f"{icons.red_dot} {e}")

            print(f"{icons.in_progress} Gathering the trace events for this query batch.")

            # Fetch periodically until timeout is expired
            time_in_secs = timeout
            step_size = 10
            while time_in_secs > 0:
                pd_df = trace.get_trace_logs()
                if not pd_df.empty:
                    for _, row in pd_df.iterrows():
                        if row['Event Class'] == 'QueryEnd' \
                        and _get_query_name(row["Text Data"]) == last_executed_query:
                            time_in_secs = 0
                            print(f"{icons.green_dot} Trace events gathered for this query batch.")
                            break
                
                if time_in_secs > 0:
                    time.sleep(step_size)
                    time_in_secs -= step_size

            trace_df = trace.stop()

            if trace_df is None:
                return (None, query_results)
            
    # Remove all rows where 'Text Data' startswith 'EVALUATE {1}'
    trace_df = trace_df[~trace_df['Text Data'].str.startswith("""EVALUATE {1}""")]

    # Add a QueryId column for the query names/ids from the 'Text Data'
    trace_df['QueryId'] = trace_df['Text Data'].apply(_get_query_name)

    # Function to replace the query text in trace events with the clean query text from the dax_queries dict.
    def replace_text_data(row):
        if row['Event Class'].startswith('Query'):
            row['Text Data'] = _get_query_text(
                query_dict = dax_queries,
                query_id = _get_query_name(row['Text Data']),
                default = row['Text Data'])
        return row

    trace_df = trace_df.apply(replace_text_data, axis=1)

    spark_trace_df = spark.createDataFrame(trace_df)

    return(spark_trace_df, query_results)


def _tag_dax_queries(
    query_dict: dict,
) -> List:
    """
    Add the query name to the query text for later identification in Profiler trace events.

    Parameters
    ----------
    query_dict : dict
        A dictionary with the query name and query text records.
        The test definitions dataframe must have the following columns.
        +----------+----------+
        | QueryId|   QueryText|
        +----------+----------+

    Returns
    -------
    list
        A list of dictionaries. Each dictionary in the list contains keys like QueryId and QueryText.
        Use a simple for loop to iterate over this list and access the QueryText for each dictionary.
    """
    import re

    query_dicts = [q.asDict() for q in query_dict]
    tag = "perflabquerynametagx"
    for q in query_dicts:
        query_text = q['QueryText']
        query_name = q['QueryId'].replace('"', '""')
        match = re.search(r'^DEFINE.*$', query_text, re.MULTILINE)
        if match:
            q['QueryText'] = query_text.replace("DEFINE",
                f"DEFINE\n var {tag} = \"{query_name}\"\n", 1)
        else:
            q['QueryText'] = f"DEFINE\n var {tag} = \"{query_name}\"\n{query_text}"


    return query_dicts

def _get_query_name(
    query_text: str
)->str:
    """
    Extracts the string assigned to the perflabquerynametagx variable from the DAX query text.

    Parameters
    ----------
    query_text : str
        The text containing the perflabquerynametagx assignment.

    Returns:
    str
        The extracted query name, or None if no match is found.
    """
    import re

    pattern = r'(?<=var perflabquerynametagx = ").*?(?=")'
    match = re.search(pattern, query_text)
    if match:
        return match.group()
    else:
        return None

def _get_query_text(
    query_dict: dict,
    query_id: str,
    default: Optional[str] = ""
) -> str:
    """
    A helper function to return the QueryText value for a given QueryId
    or a default value of the QueryId was not found.

    Parameters
    ----------
    query_dict : dict
        A dictionary with the query name and query text records.
        The records must have the following columns.
        +----------+----------+
        | QueryId|   QueryText|
        +----------+----------+
    query_id: str
        The QueryId for which to return the QueryText.
    default: str, Default = ""
        The value to return if the QueryId was not found in the dictionary.
    Returns
    -------
    str
        The QueryText for a given QueryId or the default value.
    """
    
    for query in query_dict:
        if query['QueryId'] == query_id:
            return query['QueryText']
    return default

