import sys
import io
import uuid
from contextlib import contextmanager

from typing import Optional, Tuple, List
from sempy_labs.perf_lab._test_suite import TestSuite

import sempy.fabric as fabric
import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    generate_guid,
    _create_spark_session,
    save_as_delta_table,
    )

from sempy_labs import clear_cache
from sempy_labs._refresh_semantic_model import refresh_semantic_model


def initialize_test_cycle(
    test_suite: Optional[TestSuite] = None,
    test_run_id: Optional[str] = None,
    test_description: Optional[str] = None,
) -> TestSuite:
    """
    Adds TestRunId and TestRunTimestamp fields to the test definitions in the provided TestSuite instance.

    Parameters
    ----------
    test_suite : TestSuite, default=None
        A TestSuite instance with the test definitions.
    test_run_id: str, default=None
        An optional id for the test run to be included in the test definitions.
    test_description: str, Default = None
        An optional description to be included in the test definitions.

    Returns
    -------
    TestSuite
        A test cycle-initialized TestSuite object containing the test definitions with TestRunId and TestRunTimestamp fields:
        ----+---------+----------------+
        ... |TestRunId|TestRunTimestamp|
        ----+---------+----------------+
    """
    from datetime import datetime

    if not test_run_id:
        test_run_id = generate_guid()

    test_suite.add_field("TestRunId", test_run_id)
    test_suite.add_field(
        "TestRunTimestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    test_suite.add_field("TestRunDescription", test_description)

    return test_suite


def run_test_cycle(
    test_suite: TestSuite,
    clear_query_cache: Optional[bool] = True,
    refresh_type: Optional[str] = None,
    trace_timeout: Optional[int] = 60,
    tag: Optional[str] = None,
) -> Tuple["pyspark.sql.DataFrame", dict]:
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

    for row in test_cycle_definitions_df.dropDuplicates(
        ["TargetWorkspace", "TargetDataset", "TestRunId", "TestRunTimestamp"]
    ).collect():
        target_dataset = row["TargetDataset"]
        target_workspace = row["TargetWorkspace"]
        test_run_id = row["TestRunId"]
        test_run_timestamp = row["TestRunTimestamp"]

        # Skip this row if the target semantic model is not specified.
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
                f"{icons.red_dot} Unable to resolve the target workspace '{target_workspace}' for test semantic model '{target_dataset}'. Ignoring semantic model '{target_dataset}'. Please review your test definitions."
            )
            continue
        target_workspace_name = dfW.iloc[0]["Name"]
        target_workspace_id = dfW.iloc[0]["Id"]

        # Skip this row if the target_dataset does not exist.
        dfSM = fabric.list_datasets(target_workspace_id)
        dfSM = dfSM[
            (dfSM["Dataset Name"] == target_dataset)
            | (dfSM["Dataset ID"] == target_dataset)
        ]
        if dfSM.empty:
            print(
                f"{icons.red_dot} Unable to find semantic model '{target_dataset}' in workspace '{target_workspace}'. Ignoring this semantic model. Please review your test definitions."
            )
            continue
        target_dataset_name = dfSM.iloc[0]["Dataset Name"]
        target_dataset_id = dfSM.iloc[0]["Dataset ID"]

        # Prior to tracing the DAX queries for this model, perform the requested refreshes.
        if refresh_type and refresh_type != "clearValuesFull":
            # The refresh type is supported by the refresh_semantic_model function.
            refresh_semantic_model(
                dataset=target_dataset_id,
                workspace=target_workspace_id,
                refresh_type=refresh_type,
            )
        elif refresh_type == "clearValuesFull":
            # The refresh type 'clearValuesFull' requires 2 refresh_semantic_model calls
            # 1. clearValues, 2. full
            refresh_semantic_model(
                dataset=target_dataset_id,
                workspace=target_workspace_id,
                refresh_type="clearValues",
            )
            refresh_semantic_model(
                dataset=target_dataset_id,
                workspace=target_workspace_id,
                refresh_type="full",
            )

        # Run all queries that use the current target semantic model,
        # and then merge the results with the overall cycle results.
        queries_df = test_cycle_definitions_df.filter(
            (test_cycle_definitions_df.TargetWorkspace == target_workspace)
            & (test_cycle_definitions_df.TargetDataset == target_dataset)
        )

        (trace_df, query_results) = _trace_dax_queries(
            dataset=target_dataset_name,
            dax_queries=_queries_toDict(queries_df),
            workspace=target_workspace_name,
            clear_query_cache=clear_query_cache,
            timeout=trace_timeout,
        )

        if trace_df:
            # Add test cycle info to the trace events.
            trace_df = (
                trace_df.withColumn("TestRunId", lit(test_run_id))
                .withColumn("TestRunTimestamp", lit(test_run_timestamp))
                .withColumn("DatasetId", lit(target_dataset_id))
                .withColumn("WorkspaceId", lit(target_workspace_id))
                .withColumn("Tag", lit(tag))
            )
            if cycle_results_tuple[0]:
                cycle_results_tuple = (
                    cycle_results_tuple[0].union(trace_df),
                    cycle_results_tuple[1],
                )
            else:
                cycle_results_tuple = (trace_df, cycle_results_tuple[1])

        if query_results:
            cycle_results_tuple = (
                cycle_results_tuple[0],
                cycle_results_tuple[1] | query_results,
            )

    return cycle_results_tuple


def _queries_toDict(test_definitions: "pyspark.sql.DataFrame") -> dict:
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
) -> Tuple["pyspark.sql.DataFrame", dict]:
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

    spark = _create_spark_session()
    warnings.filterwarnings("ignore", category=UserWarning)

    base_cols = [
        "EventClass",
        "EventSubclass",
        "CurrentTime",
        "NTUserName",
        "TextData",
        "ActivityID",
        "RequestID",
    ]
    begin_cols = base_cols + ["StartTime"]
    end_cols = base_cols + ["StartTime", "EndTime", "Duration", "CpuTime", "Success"]

    event_schema = {
        "QueryBegin": begin_cols,
        "QueryEnd": end_cols,
        "VertiPaqSEQueryBegin": begin_cols,
        "VertiPaqSEQueryEnd": end_cols,
        "VertiPaqSEQueryCacheMatch": base_cols,
        "DirectQueryEnd": [
            "EventClass",
            "TextData",
            "Duration",
            "StartTime",
            "EndTime",
            "ActivityID",
            "RequestID",
        ],
        "ExecutionMetrics": [
            "EventClass",
            "ApplicationName",
            "TextData",
            "ActivityID",
            "RequestID",
        ],
    }

    query_results = {}
    last_executed_query = ""

    # Establish trace connection
    with fabric.create_trace_connection(
        dataset=dataset, workspace=workspace
    ) as trace_connection:
        with trace_connection.create_trace(event_schema) as trace:
            trace.start()
            try:
                print(f"{icons.in_progress} Tracing {len(dax_queries)} dax queries...")

                # Tag and loop through the DAX queries
                tagged_dax_queries = _tag_dax_queries(dax_queries)
                for query in tagged_dax_queries:
                    query_name = query["QueryId"]
                    query_text = query["QueryText"]

                    if clear_query_cache:
                        clear_cache(dataset=dataset, workspace=workspace)

                    fabric.evaluate_dax(
                        dataset=dataset, workspace=workspace, dax_string="EVALUATE {1}"
                    )

                    result = fabric.evaluate_dax(
                        dataset=dataset, workspace=workspace, dax_string=query_text
                    )

                    # Add results to output
                    query_results[query_name] = result
                    print(
                        f"{icons.green_dot} The '{query_name}' query has completed with {result.shape[0]} rows."
                    )
                    last_executed_query = query_name

            except Exception as e:
                print(f"{icons.red_dot} {e}")

            print(
                f"{icons.in_progress} Gathering the trace events for this query batch."
            )

            # Fetch periodically until timeout is expired
            time_in_secs = timeout
            step_size = 10
            while time_in_secs > 0:
                pd_df = trace.get_trace_logs()
                if not pd_df.empty:
                    for _, row in pd_df.iterrows():
                        if (
                            row["Event Class"] == "QueryEnd"
                            and _get_query_name(row["Text Data"]) == last_executed_query
                        ):
                            time_in_secs = 0
                            print(
                                f"{icons.green_dot} Trace events gathered for this query batch."
                            )
                            break

                if time_in_secs > 0:
                    time.sleep(step_size)
                    time_in_secs -= step_size

            trace_df = trace.stop()

            if trace_df is None:
                return (None, query_results)

    # Remove all rows where 'Text Data' startswith 'EVALUATE {1}'
    trace_df = trace_df[~trace_df["Text Data"].str.startswith("""EVALUATE {1}""")]

    # Add a QueryId column for the query names/ids from the 'Text Data'
    trace_df["QueryId"] = trace_df["Text Data"].apply(_get_query_name)

    # Function to replace the query text in trace events with the clean query text from the dax_queries dict.
    def replace_text_data(row):
        if row["Event Class"].startswith("Query"):
            row["Text Data"] = _get_query_text(
                query_dict=dax_queries,
                query_id=_get_query_name(row["Text Data"]),
                default=row["Text Data"],
            )
        return row

    trace_df = trace_df.apply(replace_text_data, axis=1)

    spark_trace_df = spark.createDataFrame(trace_df)

    return (spark_trace_df, query_results)


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
        query_text = q["QueryText"]
        query_name = q["QueryId"].replace('"', '""')
        match = re.search(r"^DEFINE.*$", query_text, re.MULTILINE)
        if match:
            q["QueryText"] = query_text.replace(
                "DEFINE", f'DEFINE\n var {tag} = "{query_name}"\n', 1
            )
        else:
            q["QueryText"] = f'DEFINE\n var {tag} = "{query_name}"\n{query_text}'

    return query_dicts


def _get_query_name(query_text: str) -> str:
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
    query_dict: dict, query_id: str, default: Optional[str] = ""
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
        if query["QueryId"] == query_id:
            return query["QueryText"]
    return default


def warmup_test_models(
     test_suite: TestSuite,
) -> None:
    """
    Run all DAX queries defined in the test suite against their test models
    to ensure their data is resident in memory (warm up).

    Parameters
    ----------
    test_suite: TestSuite
        A TestSuite object with the test definitions.
        The test definitions must have the following fields.
        +---------+---------------+-------------+
        |QueryText|TargetWorkspace|TargetDataset|
        +---------+---------------+-------------+
    Returns
    -------
    None
    """
    import urllib.parse

    for row in test_suite.to_df().dropDuplicates(['TargetWorkspace', 'TargetDataset']).collect():
        target_dataset = row['TargetDataset']
        target_workspace = row['TargetWorkspace']

        # Skip this row if the target semantic model is not defined.
        if not target_dataset:
            print(f"{icons.red_dot} No test semantic model specifed as the target dataset. Ignoring this row. Please review your test definitions.")
            continue

        # URL-encode the workspace name
        filter_condition = urllib.parse.quote(target_workspace)
        dfW = fabric.list_workspaces(filter=f"name eq '{filter_condition}' or id eq '{filter_condition}'")
        if not dfW.empty:
            target_workspace_id = dfW.iloc[0]['Id']
        else:
            print(f"{icons.red_dot} Unable to resolve workspace '{target_workspace}' for test semantic model '{target_dataset}'. Ignoring this row. Please review your test definitions.")
            continue 

        # Skip this row if the target_dataset does not exist.
        dfSM = fabric.list_datasets(target_workspace_id)
        dfSM = dfSM[
            (dfSM["Dataset Name"] == target_dataset)
            | (dfSM["Dataset ID"] == target_dataset)
        ]
        if dfSM.empty:
            print(f"{icons.red_dot} Unable to find the test semantic model '{target_dataset}' in workspace '{target_workspace}'. Ignoring this row. Please review your test definitions.")
            continue

        # Filter the test suite and select the QueryText column
        df = test_suite.to_df()
        queries_df = df.filter((df.TargetWorkspace == target_workspace) & (df.TargetDataset == target_dataset)).select("QueryText")
        for row in queries_df.collect():
            fabric.evaluate_dax(
                dataset=target_dataset, 
                workspace=target_workspace, 
                dax_string=row.QueryText)
        print(f"{icons.green_dot} {queries_df.count()} queries executed to warm up semantic model '{target_dataset}' in workspace '{target_workspace}'.")


def refresh_test_models(
    test_suite: TestSuite,
    refresh_type: str = "full",
) -> None:
    """
    Refreshes the test models referenced in the test definitions of the provided test suite.

    Parameters
    ----------
    test_suite : TestSuite
        A TestSuite object with the test definitions.
        The test definitions must have the following fields.
        +---------+---------------+-------------+
        |QueryText|TargetWorkspace|TargetDataset|
        +---------+---------------+-------------+
    refresh_type : str, Default = full
        The type of processing to perform for each test semantic model.
        Types align with the TMSL refresh command types: full, clearValues, calculate, dataOnly, automatic, and defragment. 
        The add type isn't supported.
        In addition, refresh_type can be set to clearValuesFull, which performs a clearValues refresh followed by a full refresh.

    """
    import urllib.parse

    test_definitions = test_suite.to_df()
    for row in test_definitions.dropDuplicates(['TargetWorkspace', 'TargetDataset']).collect():
        target_dataset = row['TargetDataset']
        target_workspace = row['TargetWorkspace']

        # Skip this row if the target semantic model is not defined.
        if not target_dataset:
            print(f"{icons.red_dot} No test semantic model specifed as the target dataset. Ignoring this row. Please review your test definitions.")
            continue

        # URL-encode the workspace name
        filter_condition = urllib.parse.quote(target_workspace)
        dfW = fabric.list_workspaces(filter=f"name eq '{filter_condition}' or id eq '{filter_condition}'")
        if not dfW.empty:
            target_workspace_id = dfW.iloc[0]['Id']
        else:
            print(f"{icons.red_dot} Unable to resolve workspace '{target_workspace}' for test semantic model '{target_dataset}'. Ignoring this row. Please review your test definitions.")
            continue 

        # Skip this row if the target_dataset does not exist.
        dfSM = fabric.list_datasets(target_workspace_id)
        dfSM = dfSM[
            (dfSM["Dataset Name"] == target_dataset)
            | (dfSM["Dataset ID"] == target_dataset)
        ]
        if dfSM.empty:
            print(f"{icons.red_dot} Unable to find the test semantic model '{target_dataset}' in workspace '{target_workspace}'. Ignoring this row. Please review your test definitions.")
            continue         

        target_dataset_id = dfSM.iloc[0]["Dataset ID"]

        if not refresh_type:
            print(f"{icons.red_dot} Unable to refresh test definitions because no refresh type was specified.")
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
        else:
            # The refresh type is supported by the refresh_semantic_model function.
            refresh_semantic_model( 
                dataset=target_dataset_id, 
                workspace=target_workspace_id, 
                refresh_type=refresh_type)


class ExecutionTracker:
    """
    ExecutionTracker is a context manager that logs the success or error of a code block execution into a Delta table.
    It can be used in conjunction with the test cycle functions to keep track execution status in order 
    to identify accurate versus failed/incomplete test results. 

    Attributes:
        table_name : str
            The name of the Delta table where logs will be stored.
        run_id : str
            A unique identifier for the execution run.
        description : str
            A description of the execution run.
        spark : SparkSession
            The Spark session used for executing SQL commands.
        start_time : datetime
            The start time of the execution run.
        end_time : datetime
            The end time of the execution run.

    Methods:
        log_event(status, message)
            Logs an event with the given status and message.
    """    
    def __init__(self, table_name, run_id = str(uuid.uuid4()), description = ""):
        """
        Initializes the ExecutionTracker with the specified table name.

        Args:
            table_name : str
                The name of the Delta table where logs will be stored.
            run_id : str, Default = random id
                The id of the execution run.
            description : str
                A description of the execution run.
        """

        self.table_name = table_name
        self.run_id = run_id
        self.description = description
        self.spark = _create_spark_session()
        self.start_time = None
        self.end_time = None
        self.output = io.StringIO()

    def __enter__(self):
        """
        Enters the runtime context related to this object and logs the start time.

        Returns:
            self: The ExecutionTracker instance.
        """
        self.start_time = self.spark.sql("SELECT current_timestamp()").collect()[0][0]
        
        self.output = io.StringIO()
        self.dual_output = self.DualOutput(self.output)
        self.dual_output.__enter__()

        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """
        Exits the runtime context related to this object, logging the success or error of the code block execution and the end time.

        Args:
            exc_type : type
                The exception type.
            exc_value : Exception
                The exception instance.
            exc_traceback : traceback
                The traceback object.

        Returns:
            bool: False to propagate the exception if any.
        """
        import traceback

        self.end_time = self.spark.sql("SELECT current_timestamp()").collect()[0][0]
        
        output = self.output.getvalue()
        if output:
            self.log_event("OUTPUT", output)

        if exc_type is None:
            self.log_event("SUCCESS", "Execution completed successfully.")
        else:
            error_message = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
            self.log_event("ERROR", error_message)
        return False  # Propagate exception if any

    @contextmanager
    def DualOutput(self, output_stream):
        """
        Context manager to duplicate output to both the notebook and a given output stream.

        Args:
            output_stream : io.StringIO
                The output stream to capture the output.
        """
        class DualWriter:
            def __init__(self, stream1, stream2):
                self.stream1 = stream1
                self.stream2 = stream2

            def write(self, message):
                self.stream1.write(message)
                self.stream2.write(message)

            def flush(self):
                self.stream1.flush()
                self.stream2.flush()

        original_stdout = sys.stdout
        original_stderr = sys.stderr
        sys.stdout = DualWriter(original_stdout, output_stream)
        sys.stderr = DualWriter(original_stderr, output_stream)
        
        try:
            yield
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr

    def log_event(self, status, message):
        """
        Logs an event with the given status and message into the Delta table.

        Args:
            status : str
                The status of the event (e.g., "SUCCESS" or "ERROR").
            message : str
                The message to log.
        """
        log_df = self.spark.createDataFrame([(self.start_time, self.end_time, status, "ExecutionTracker", message, self.run_id, self.description)], 
                                            ["StartTime", "EndTime", "Status", "FunctionName", "Message", "RunId", "Description"])

        save_as_delta_table(
            dataframe = log_df,
            delta_table_name = self.table_name,
            write_mode = 'append',
        )