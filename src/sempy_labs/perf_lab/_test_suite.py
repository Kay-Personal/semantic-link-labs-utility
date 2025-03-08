from uuid import UUID
from typing import Optional
import sempy.fabric as fabric

import sempy_labs._icons as icons
from sempy_labs._helper_functions import (
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    save_as_delta_table,
    _read_delta_table
)

class TestDefinition:
    """
        A test definition must have at least the following fields, but can also have additional arbitrary fields.
        +----------+----------+----------------+-------------+---------------+-------------+---------------+-------------------+--------------+
        | QueryId|   QueryText| MasterWorkspace|MasterDataset|TargetWorkspace|TargetDataset| DatasourceName|DatasourceWorkspace|DatasourceType|
        +----------+----------+----------------+-------------+---------------+-------------+---------------+-------------------+--------------+
    """    
    def __init__(self, **kwargs):
        self.fields = ['QueryId', 'QueryText', 'MasterWorkspace', 'MasterDataset', 
                       'TargetWorkspace', 'TargetDataset', 'DatasourceName', 
                       'DatasourceWorkspace', 'DatasourceType']
        for field in self.fields:
            setattr(self, field, kwargs.get(field, None))
        # Set any additional fields
        for key, value in kwargs.items():
            if key not in self.fields:
                setattr(self, key, value)

    def add(self, key, value):
        setattr(self, key, value)

    def remove(self, key):
        if hasattr(self, key):
            delattr(self, key)

    def get_keys(self):
        return [key for key in self.__dict__.keys() if key != 'fields']

    def get_values(self):
        return tuple(value for key, value in self.__dict__.items() if key != 'fields')

    def to_schema(self):
        from pyspark.sql.types import StructType, StructField, StringType
        schema_fields = [StructField(field, StringType(), True) for field in self.get_keys()]
        return StructType(schema_fields)


class TestSuite:
    """
        A test suite consists of an array of test definitions 
        and provides helpful methods to load and persist them in Delta tables.
    """  
    def __init__(self, test_definitions=None):
        """
            Initializes a test suite instance with an array of test definitions
            or an empty array if no test definitions were provided.
        """
        if test_definitions is None:
            test_definitions = []
        self.test_definitions = test_definitions

    def add_test_definition(self, test_definition):
        """
            Adds a new item to the array of test definitions.
        """
        self.test_definitions.append(test_definition)

    def remove_test_definition(self, test_definition):
        """
            Removes an item from the array of test definitions.
        """
        if test_definition in self.test_definitions:
            self.test_definitions.remove(test_definition)

    def clear(self):
        """
            Removes all test definitions.
        """
        self.test_definitions = []

    def add_field(self, key, value):
        """
            Adds a new field to all test definitions 
            and sets it to the specified value.
        """
        for test_def in self.test_definitions:
            test_def.add(key, value)

    def remove_field(self, key):
        """
            Removes a field from all test definitions.
        """
        for test_def in self.test_definitions:
            test_def.remove(key)

    def get_schema(self):
        """
            Returns a PySpark schema based on the fields of the first test definition.
            All test definitions are expected to have the same fields.
        """
        if self.test_definitions:
            return self.test_definitions[0].to_schema()
        return None

    def to_df(self):
        """
            Returns a PySpark dataframe with the test definitions.
        """
        from pyspark.sql import SparkSession, Row
        from pyspark.sql.types import StructType

        spark = SparkSession.builder.getOrCreate()
        schema = self.get_schema()
        if schema:
            rows = [Row(*test_def.get_values()) for test_def in self.test_definitions]
            return spark.createDataFrame(rows, schema)
        else: 
            empty_df = spark.createDataFrame
            return spark.createDataFrame(
                spark.sparkContext.emptyRDD(), StructType([]))

    def load(self, 
            delta_table: str,
            filter_expression: Optional [str ] = None,
            lakehouse: Optional [str | UUID] = None,
            workspace: Optional [str | UUID] = None,
        ):
        """
        Loads test definitions from a Delta table in a Fabric lakehouse 
        and adds them to any existing test definitions.
        The Delta table must have at least the following columns, but can also have additional arbitrary columns.
        +----------+----------+----------------+-------------+---------------+-------------+---------------+-------------------+--------------+
        | QueryId|   QueryText| MasterWorkspace|MasterDataset|TargetWorkspace|TargetDataset| DatasourceName|DatasourceWorkspace|DatasourceType|
        +----------+----------+----------------+-------------+---------------+-------------+---------------+-------------------+--------------+

        Parameters
        ----------
        delta_table : str
            The name or path of the delta table.
        filter_expression : str, default=None
            A PySpark filter expression to narrow down the test definitions that should be loaded.
        lakehouse : uuid.UUID, default=None
            The Fabric lakehouse ID.
            Defaults to None which resolves to the lakehouse attached to the notebook.
        workspace : uuid.UUID, default=None
            The Fabric workspace ID where the specified lakehouse is located.
            Defaults to None which resolves to the workspace of the attached lakehouse
            or if no lakehouse attached, resolves to the workspace of the notebook.
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        df = _read_delta_table(
            path = delta_table,
            lakehouse = lakehouse,
            workspace = workspace,
        )

        if filter_expression:
            df = df.filter(filter_expression)

        for row in df.collect():
            test_definition = TestDefinition(**row.asDict())
            self.add_test_definition(test_definition)

    def save_as(self,     
            delta_table_name: str,
            lakehouse: Optional [str | UUID] = None,
            workspace: Optional [str | UUID] = None,
        ):
        """
        Saves a spark dataframe as a delta table in a Fabric lakehouse.

        Parameters
        ----------
        delta_table_name : str
            The name of the delta table.
        lakehouse : uuid.UUID
            The Fabric lakehouse ID.
            Defaults to None which resolves to the lakehouse attached to the notebook.
        workspace : uuid.UUID
            The Fabric workspace ID where the specified lakehouse is located.
            Defaults to None which resolves to the workspace of the attached lakehouse
            or if no lakehouse attached, resolves to the workspace of the notebook.
        """

        save_as_delta_table(
            dataframe = self.to_df(),
            delta_table_name = delta_table_name,
            lakehouse = lakehouse,
            workspace = workspace,
            write_mode = 'overwrite',
        )
    
    def merge(self, other):
        """ 
            Merges this TestSuite instance with another TestSuite instance.
        """
        if not isinstance(other, TestSuite):
            raise ValueError("Can only merge with another TestSuite instance")
        self.test_definitions.extend(other.test_definitions)

    def from_trace_events(
            self,
            master_dataset: str | UUID,
            target_dataset_prefix: str | UUID,
            query_id_prefix: Optional[str] = "Query",
            master_workspace: Optional[str | UUID] = None,
            target_workspace: Optional[str | UUID] = None,
            data_source: Optional[str | UUID] = None,
            data_source_workspace: Optional[str | UUID] = None,
            data_source_type: Optional[str] = "Lakehouse",
            timeout: Optional[int] = 300,
        ):
        """
        Generates test definitions using the specified input parameters and captured DAX query trace events.

        Parameters
        ----------
        master_dataset : str | uuid.UUID, default=None
            The master semantic model name or ID. 
            This is the semantic model for which the query trace events are captured.
        target_dataset_prefix : str | uuid.UUID
            The semantic model name or ID designating the model that the 
            test cycle should use to run a DAX query. This function generates
            a unique name for each unique DAX query in the form of {target_dataset_prefix}_{sequence_number}
        query_id_prefix : str, default="Query"
            The prefix for the query id to identify each DAX query. The generated query id
            has the form {query_id_prefix}{sequence_number}.
        master_workspace : str | uuid.UUID, default=None
            The Fabric workspace name or ID where the master dataset is located.
            Defaults to None which resolves to the workspace of the attached lakehouse
            or if no lakehouse attached, resolves to the workspace of the notebook.
        target_workspace : str | uuid.UUID, default=None
            The Fabric workspace name or ID where the target dataset is located.
            Defaults to None which resolves to the workspace of the attached lakehouse
            or if no lakehouse attached, resolves to the workspace of the notebook.
        data_source : str | uuid.UUID, default=None
            The name or ID of the lakehouse or other artifact that serves as the data source for the target_dataset.
            Defaults to None which resolves to the lakehouse attached to this notebook.
            If no lakehouse is attached to this notebook, you must provide the data_source parameter.
        data_source_workspace : str | uuid.UUID, default=None
            The Fabric workspace name or ID where the data source is located.
            Defaults to None which resolves to the workspace of the attached lakehouse,
            or if no lakehouse attached, resolves to the workspace of the notebook.
        data_source_type : str, default=Lakehouse
            The type of the data source. Currently, the only supported type is Lakehouse.
        timeout : int, default=30
            The max time duration to capture trace events after execution of the DAX queries.
        """
        import warnings
        import time
        from tqdm.auto import tqdm
        from sempy_labs.perf_lab._lab_infrastructure import (
            _get_workspace_name_and_id,
            _get_dataset_name_and_id,
            _get_lakehouse_name_and_id
        )

        # Parameter validation
        if data_source_type != "Lakehouse":
            raise ValueError("Unrecognized data source type specified. The only valid option for now is 'Lakehouse'.")
        if not master_dataset:
            raise ValueError("The master_dataset must be specified. The master dataset is required to capture trace events.")
        if data_source_workspace and not data_source:
            raise ValueError("The data_source must be specified if a data_source_workspace was provided.")

        if not master_workspace:
            (master_workspace_name, master_workspace_id) = resolve_workspace_name_and_id()
        else:
            (master_workspace_name, master_workspace_id) = _get_workspace_name_and_id(master_workspace)

        if not target_workspace:
            (target_workspace_name, target_workspace_id) = resolve_workspace_name_and_id()
        else:
            (target_workspace_name, target_workspace_id) = _get_workspace_name_and_id(target_workspace)

        if not data_source and not data_source_workspace:
            (data_source_workspace_name, data_source_workspace_id) = resolve_workspace_name_and_id()
            (data_source_name, data_source_id) = resolve_lakehouse_name_and_id(workspace=data_source_workspace_id)
        elif data_source and not data_source_workspace:
            (data_source_workspace_name, data_source_workspace_id) = resolve_workspace_name_and_id()
            (data_source_name, data_source_id) = _get_lakehouse_name_and_id(lakehouse=data_source, workspace=data_source_workspace_id)
        else:
            (data_source_workspace_name, data_source_workspace_id) = _get_workspace_name_and_id(data_source_workspace)
            (data_source_name, data_source_id) = _get_lakehouse_name_and_id(lakehouse=data_source, workspace=data_source_workspace_id)

        (master_dataset_name, master_dataset_id) = _get_dataset_name_and_id(dataset=master_dataset, workspace=master_workspace_id)

        warnings.filterwarnings("ignore", category=UserWarning)

        event_schema = {
            "QueryBegin": ["TextData"],
        }
        with fabric.create_trace_connection(dataset=master_dataset_id, workspace=master_workspace_id) as trace_connection:
            with trace_connection.create_trace(event_schema) as trace:
                trace.start()
                # Loop until the timeout expires or until a query with '{"Stop"}' in the query text is received.
                time_in_secs = timeout
                step_size = 1
                print(f"{icons.in_progress} Entering a trace loop for up to {time_in_secs} seconds to capture DAX queries executed against the '{master_dataset_name}' semantic model. Execute 'EVALUATE {{\"Stop\"}}' to exit the trace loop.")
                
                # Initialize tqdm progress bar with green bar color
                with tqdm(total=time_in_secs, desc="Capturing DAX queries (0 captured)", colour="green") as pbar:
                    while time_in_secs > 0:
                        df = trace.get_trace_logs()
                        if not df.empty:
                            pbar.set_description(f"Capturing DAX queries ({len(df)} captured)")
                            for _, row in df.iterrows():
                                if row['Text Data'].find("""{"Stop"}""") != -1:                            
                                    time_in_secs = 0
                            
                        if time_in_secs > 0:
                            time.sleep(step_size)
                            time_in_secs -= step_size                 
                            pbar.update(step_size)                     
                    
                df = trace.stop()
                if not df.empty:
                    row_count = len(df[~df['Text Data'].str.contains('{"Stop"}')])
                    print(f"{icons.green_dot} Trace loop exited. Trace stopped. {row_count} DAX queries captured.")
                else:
                    print(f"{icons.yellow_dot} Trace loop exited. Trace stopped. 0 DAX queries captured.")

                i = 0
                for _, row in df.iterrows():
                    if row['Text Data'].find("""{"Stop"}""") == -1:
                        i += 1
                        self.add_test_definition(
                            TestDefinition(
                                QueryId=f"{query_id_prefix}{i}", 
                                QueryText=row['Text Data'].replace("\n\n", "\n"), 
                                MasterWorkspace = master_workspace_name,
                                MasterDataset = master_dataset_name,
                                TargetWorkspace = target_workspace_name,
                                TargetDataset= f"{target_dataset_prefix}_{i}",
                                DatasourceName = data_source_name,
                                DatasourceWorkspace = data_source_workspace_name,
                                DatasourceType = data_source_type))



def _get_test_definitions(
    dax_queries: list[str] | list[(str, str)],
    target_dataset: str | UUID,
    target_workspace: Optional[str | UUID] = None,
    master_dataset: Optional[str | UUID] = None,
    master_workspace: Optional[str | UUID] = None,
    data_source: Optional[str | UUID] = None,
    data_source_workspace: Optional[str | UUID] = None,
    data_source_type: Optional[str] = "Lakehouse",

) -> TestSuite:
    """
    Generates a TestSuite instance with test definitions based on a list of DAX queries
    and other provided information.

    Parameters
    ----------
    dax_queries: list[str]
        A predefined list of DAX queries.
        This can be a simple list of query expressions,
        or a list of (Query_Id, Query_Text) tuples.
    target_dataset : str | uuid.UUID
        The semantic model name or ID designating the model that the 
        test cycle should use to run the DAX queries.
    target_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where the target dataset is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    master_dataset : str | uuid.UUID, default=None
        The master semantic model name or ID for the target_dataset. If not 
        specified, the test cycle cannot clone the master to create the target_dataset.
        In this case, the target_dataset must already exist.
    master_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where the master dataset is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    data_source : str | uuid.UUID, default=None
        The name or ID of the lakehouse or other artifact that serves as the data source for the target_dataset.
        Defaults to None which resolves to the lakehouse or warehouse referenced in the data source shared expression.
    data_source_workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID where the data source is located.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    data_source_type : str, default=Lakehouse
        The type of the data source. Currently, the only supported type is Lakehouse.

    Returns
    -------
    TestSuite
        A TestSuite object with the test definitions based on the specified DAX queries.
    """
    from sempy_labs.perf_lab._lab_infrastructure import (
        _get_workspace_name_and_id,
        _get_dataset_name_and_id,
        _get_lakehouse_name_and_id
    )

    # Parameter validation
    if data_source_type != "Lakehouse":
        raise ValueError("Unrecognized data source type specified. The only valid option for now is 'Lakehouse'.")

    (target_workspace_name, target_workspace_id) = _get_workspace_name_and_id(target_workspace)
    (master_workspace_name, master_workspace_id) = _get_workspace_name_and_id(master_workspace)
    (data_source_workspace_name, data_source_workspace_id) = _get_workspace_name_and_id(data_source_workspace)

    (target_dataset_name, target_dataset_id) = _get_dataset_name_and_id(dataset=target_dataset, workspace=target_workspace_id)
    (master_dataset_name, master_dataset_id) = _get_dataset_name_and_id(dataset=master_dataset, workspace=master_workspace_id)

    (data_source_name, data_source_id) = _get_lakehouse_name_and_id(lakehouse=data_source, workspace=data_source_workspace_id)

    test_suite = TestSuite()
    for i in range(len(dax_queries)):
        q = dax_queries[i]
        if isinstance(q, str):          
            test_suite.add_test_definition(
                TestDefinition(
                    QueryId=f"Q{i}", 
                    QueryText=q, 
                    MasterWorkspace = master_workspace_name,
                    MasterDataset = master_dataset_name,
                    TargetWorkspace = target_workspace_name,
                    TargetDataset= target_dataset_name,
                    DatasourceName = data_source_name,
                    DatasourceWorkspace = data_source_workspace_name,
                    DatasourceType = data_source_type))
        elif isinstance(q, tuple):
            test_suite.add_test_definition(
                TestDefinition(
                    QueryId=q[0], 
                    QueryText=q[1], 
                    MasterWorkspace = master_workspace_name,
                    MasterDataset = master_dataset_name,
                    TargetWorkspace = target_workspace_name,
                    TargetDataset= target_dataset_name,
                    DatasourceName = data_source_name,
                    DatasourceWorkspace = data_source_workspace_name,
                    DatasourceType = data_source_type))
            
    return test_suite
