from uuid import UUID
from typing import Optional, Tuple, Callable, List

import sempy.fabric as fabric
from sempy.fabric.exceptions import FabricHTTPException

import sempy_labs._icons as icons
from sempy_labs.lakehouse._get_lakehouse_tables import get_lakehouse_tables
from sempy_labs.perf_lab._test_suite import TestSuite
from sempy_labs._helper_functions import (
    _get_or_create_workspace,
    _get_or_create_lakehouse,
    resolve_workspace_name_and_id,
    resolve_lakehouse_name_and_id,
    resolve_dataset_name_and_id,
)


TableGeneratorCallback = Callable[[UUID, UUID, dict], None]

def provision_lakehouses(
    test_suite: TestSuite,
    capacity: Optional[str | UUID] = None,
    table_properties: Optional[dict] = None,
    table_generator: Optional[TableGeneratorCallback] = None,
    workspace_description: Optional[str] = "A master workspace with a sample lakehouse and a Direct Lake semantic model that uses the Delta tables from the sample lakehouse.",
    lakehouse_description: Optional[str] = "A lakehouse with automatically generated sample Delta tables.",
)  -> List[Tuple[UUID, UUID]]:
    """
    Creates lakehouses referenced in the test definitions of a test suite as data sources if they don't exist,
    and returns the workspace IDs and Lakehouse IDs for all referenced lakehouses.

    Parameters
    ----------
    test_suite : TestSuite
        A TestSuite object with test definitions referencing lakehouses as data sources.
    capacity : str | uuid.UUID, default=None
        The name or ID of the capacity on which to place the new workspace.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.
    table_properties: dict, default=None
        A dictionary of property values that the provision_perf_lab_lakehouse function passes to the table_generator function.
        The keys and values in the dictionary are specific to the table_generator function.
    table_generator : TableGeneratorCallback, default=None
        A callback function to generate and persist the actual Delta tables in the lakehouse.
    workspace_description : str, default="A master workspace with a sample lakehouse and a Direct Lake semantic model that uses the Delta tables from the sample lakehouse."
        A description for the workspace to be applied when the workspace is created.
    lakehouse_description : str, default="A lakehouse with automatically generated sample Delta tables."
        A description for the lakehouse to be applied when the lakehouse is created.

    Returns
    -------
    List[Tuple[uuid.UUID, uuid.UUID]]:
        A list of tuples for the provisioned workspace IDs and lakehouse IDs.
    """
    id_pairs = []
    test_definitions = test_suite.to_df()
    lakehouses_df = test_definitions.filter(
        test_definitions["DatasourceType"] == "Lakehouse"
        ).dropDuplicates(['DatasourceName', 'DatasourceWorkspace'])
    if lakehouses_df.count() > 0:
        for lhs in lakehouses_df.dropDuplicates(['DatasourceName', 'DatasourceWorkspace']).collect():
            # Catch all exceptions so that the loop continues even when an error occurs for a particular lakehouse.
            try:
                (workspace_id, lakehouse_id) = provision_lakehouse(
                        workspace = lhs['DatasourceWorkspace'], 
                        capacity = capacity,
                        lakehouse = lhs['DatasourceName'],
                        table_properties=table_properties,
                        table_generator=table_generator,
                        workspace_description = workspace_description,
                        lakehouse_description = lakehouse_description,
                    )
                id_pairs.append((workspace_id, lakehouse_id))
            except Exception as e:
                print(f"{icons.red_dot} {e}")
    else:
        print(f"{icons.red_dot} No lakehouses found in the test definitions.")

    return id_pairs
    
def provision_lakehouse(
    workspace: Optional[str | UUID] = None,
    capacity: Optional[str | UUID] = None,
    lakehouse: Optional[str | UUID] = None,
    table_properties: Optional[dict] = None,
    table_generator: Optional[TableGeneratorCallback] = None,
    workspace_description: Optional[str] = "A master workspace with a sample lakehouse and a Direct Lake semantic model that uses the Delta tables from the sample lakehouse.",
    lakehouse_description: Optional[str] = "A lakehouse with automatically generated sample Delta tables.",
)  -> Tuple[UUID, UUID]:
    """
    Generates sample data for a date table.

    Parameters
    ----------
    workspace : str | uuid.UUID, default=None
        The Fabric workspace name or ID.
        Defaults to None which resolves to the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the workspace of the notebook.
    capacity : str | uuid.UUID, default=None
        The name or ID of the capacity on which to place the new workspace.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.
    lakehouse : str | uuid.UUID, default=None
        The name or ID of the lakehouse.
        Defaults to None which resolves to the lakehouse attached to the notebook.
    table_properties: dict, default=None
        A dictionary of property values that the provision_perf_lab_lakehouse function passes to the table_generator function.
        The keys and values in the dictionary are specific to the table_generator function.
    table_generator : TableGeneratorCallback, default=None
        A callback function to generate and persist the actual Delta tables in the lakehouse.
    workspace_description : str, default="A master workspace with a sample lakehouse and a Direct Lake semantic model that uses the Delta tables from the sample lakehouse."
        A description for the workspace to be applied when the workspace is created.
    lakehouse_description : str, default="A lakehouse with automatically generated sample Delta tables."
        A description for the lakehouse to be applied when the lakehouse is created.

    Returns
    -------
    Tuple[uuid.UUID, uuid.UUID]
        A tuple of the provisioned workspace and lakehouse IDs.
    """

    # Resolve the workspace name and id and provision a workspace if it doesn't exist.
    (workspace_name, workspace_id) = _get_or_create_workspace(
        workspace = workspace, capacity=capacity,
        description = workspace_description)

    # Resolve the lakehouse name and id and provision a lakehouse if it doesn't exist.
    (lakehouse_name, lakehouse_id) = _get_or_create_lakehouse(
        lakehouse = lakehouse, workspace = workspace_id,
        description = lakehouse_description)

    # Call the provided callback function to generate the Delta tables.
    if table_generator:
        table_generator(
            workspace_id,
            lakehouse_id,
            table_properties)
        
    return(workspace_id, lakehouse_id)

def deprovision_lakehouses(
    test_suite: TestSuite,
)->None:
    """
    Deprovisions lakehouses listed in the test definitions.
    These lakehouses may contain tables other then the perf lab tables, 
    which will all be removed together with the lakehouses.

    Parameters
    ----------
    test_suite : TestSuite
        A TestSuite object with the semantic model and data source definitions, such as the a test suite returned by the _get_test_definitions_df() function.

    Returns
    -------
    None
    """
    import notebookutils
    
    test_definitions = test_suite.to_df()
    lakehouses_df = test_definitions.filter(
        test_definitions["DatasourceType"] == "Lakehouse"
        ).dropDuplicates(['DatasourceName', 'DatasourceWorkspace'])
    
    if lakehouses_df.count() > 0:
        print(f"{icons.in_progress} Deleting '{lakehouses_df.count()}' lakehouses.")

        for lhs in lakehouses_df.dropDuplicates(['DatasourceName', 'DatasourceWorkspace']).collect():
            # Catch all exceptions so that the loop continues even when an error occurs for a particular lakehouse.
            try:
                lakehouse_name = lhs['DatasourceName']
                (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace=lhs['DatasourceWorkspace'])

                notebookutils.lakehouse.delete(lakehouse_name, workspace_id)
                print(f"{icons.checked} Lakehouse '{lakehouse_name}' in workspace '{workspace_name}' deleted successfully.")
            except Exception as e:
                print(f"{icons.red_dot} {e}")
    else:
        print(f"{icons.red_dot} No lakehouses found in the test definitions.")

MetadataGeneratorCallback = Callable[[str, UUID, bool], None]

def provision_master_semantic_models(
    test_suite: TestSuite,
    semantic_model_mode: Optional[str] = "OneLake",
    overwrite: Optional[bool] = False,
    metadata_generator: Optional[MetadataGeneratorCallback] = None,
) -> List[Tuple[str, UUID]]:
    """
    Creates master sematnic models referenced in the test definitions of a test suite
    if they use a lakehouse as their data source 
    and returns their names and IDs.

    Parameters
    ----------
    test_suite : TestSuite
        A TestSuite object with test definitions referencing master semantic models to be created.
    semantic_model_mode : str, Default = OneLake. Options: 'SQL', 'OneLake'.
        An optional parameter to specify the mode of the semantic model. Two modes are supported: SQL and OneLake. By default, the function generates a Direct Lake model in OneLake mode.
    overwrite : bool, default=False
        If set to True, overwrites the existing semantic model if it already exists.
    metadata_generator : MetadataGeneratorCallback, default = None
        A callback function to apply specific metadata to a semantic model.

    Returns
    -------
    List[Tuple[str, uuid.UUID]]:
        A list of tuples for the provisioned semantic model names and IDs.
    """
    name_id_pairs = []
    test_definitions = test_suite.to_df()

    masters_df = test_definitions.filter(
        test_definitions["DatasourceType"] == "Lakehouse"
        ).dropDuplicates(['MasterDataset', 'MasterWorkspace', 'DatasourceName'])
    if masters_df.count() > 0:
        for m in masters_df.collect():
            # Catch all exceptions so that the loop continues even when an error occurs for a particular model.
            try:
                (master_dataset_name, master_dataset_id) = provision_semantic_model(
                    workspace = m['MasterWorkspace'], 
                    lakehouse=m['DatasourceName'], 
                    semantic_model_name = m['MasterDataset'],
                    semantic_model_mode = semantic_model_mode,
                    overwrite = overwrite,
                    metadata_generator = metadata_generator,
                    )
                name_id_pairs.append((master_dataset_name, master_dataset_id))
            except Exception as e:
                print(f"{icons.red_dot} {e}")
    else:
        print(f"{icons.red_dot} No master semantic model references found in the test definitions.")

    return name_id_pairs

def provision_semantic_model(
    workspace: str | UUID,
    lakehouse: str | UUID,
    semantic_model_name: str,
    semantic_model_mode: Optional[str] = "OneLake",
    overwrite: Optional[bool] = False,
    metadata_generator: Optional[MetadataGeneratorCallback] = None,
) -> Tuple[str, UUID]:
    """
    Creates a semantic model in Direct Lake mode in the specified workspace using the specified lakehouse as the data source.
    Assumes a specific structure of Delta tables provisioned using a metadata generator callback functions.

    Parameters
    ----------
    workspace : str | uuid.UUID
        The Fabric workspace name or ID where the semantic model should be created.
        The workspace must be specified and must exist or the function fails with a WorkspaceNotFoundException.
    lakehouse : str | uuid.UUID
        The name or ID of the lakehouse that the semantic model should use as the data source.
        The lakehouse must be specified and must exist in the specified workspace or the function fails with a ValueException stating that the lakehouse was not found.
    semantic_model_name : str
        The name of the semantic model. The semantic model name must be specified. 
        If a model with the same name already exists, the function fails with a ValueException due to a naming conflict. 
    semantic_model_mode : str, Default = OneLake. Options: 'SQL', 'OneLake'.
        An optional parameter to specify the mode of the semantic model. Two modes are supported: SQL and OneLake. By default, the function generates a Direct Lake model in OneLake mode.
    overwrite : bool, default=False
        If set to True, overwrites the existing semantic model if it already exists.
    metadata_generator : MetadataGeneratorCallback, default = None
        A callback function to apply specific metadata to a semantic model.

    Returns
    -------
    Tuple[str, uuid.UUID]
        A tuple holding the name and ID of the created semantic model.
    """
    from sempy_labs.tom import connect_semantic_model
    from sempy_labs import (
        refresh_semantic_model, 
        directlake as dl,
        lakehouse as lh
    )

    # Verify that the mode is valid.
    semantic_model_modes = ["SQL", "ONELAKE"]
    semantic_model_mode = semantic_model_mode.upper()
    if semantic_model_mode not in semantic_model_modes:
        raise ValueError(
            f"{icons.red_dot} Invalid semantic model mode. Valid options: {semantic_model_modes}."
        )

    # Make sure the workspace exists. Raises WorkspaceNotFoundException otherwise.
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)

    # Raises a ValueError if there's no lakehouse with the specified name in the workspace.
    (lakehouse_name, lakehouse_id) = resolve_lakehouse_name_and_id(
        lakehouse=lakehouse, workspace=workspace_name)

    # Create the intial semantic model
    tables = lh.get_lakehouse_tables(lakehouse=lakehouse_id, workspace=workspace_id)
    table_names = tables['Table Name'].tolist()
    print(f"{icons.table_icon} {len(table_names)} tables found in lakehouse '{lakehouse_name}'.")

    # Create the intial semantic model
    print(f"{icons.in_progress} Creating Direct Lake semantic model '{semantic_model_name}' in workspace '{workspace_name}'.")
    dl.generate_direct_lake_semantic_model(
        workspace=workspace_id, 
        lakehouse=lakehouse_name,
        dataset=semantic_model_name,
        lakehouse_tables=table_names,
        overwrite=overwrite,
        refresh=False)
    
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(
        dataset=semantic_model_name, workspace=workspace_id)

    print(f"{icons.in_progress} Adding final touches to Direct Lake semantic model '{semantic_model_name}' in workspace '{workspace_name}'.")
    with connect_semantic_model(dataset=dataset_name, workspace=workspace_id, readonly=False) as tom:
        # If the semantic_model_mode is OneLake,
        # convert the data access expression in the model to Direct Lake 
        if semantic_model_mode == semantic_model_modes[1]:
            expression_name = "DatabaseQuery"
            expr = _generate_onelake_shared_expression(workspace_id, lakehouse_id)       

            if not any(e.Name == expression_name for e in tom.model.Expressions):
                tom.add_expression(name=expression_name, expression=expr)
            else:
                tom.model.Expressions[expression_name].Expression = expr
            
            # Also remove the schemaName property from all the partitions
            for t in tom.model.Tables:
                for p in t.Partitions:
                    p.Source.SchemaName = ""            
            
        print(f"{icons.checked} Direct Lake semantic model '{semantic_model_name}' converted to Direct Lake on OneLake mode.")

    if metadata_generator:
        metadata_generator(
            dataset_name,
            workspace_id,
            (semantic_model_mode == semantic_model_modes[1])
        )

    refresh_semantic_model(
        dataset=semantic_model_name, 
        workspace=workspace_id,
        retry_count = 3,
    )

    print(f"{icons.green_dot} Direct Lake semantic model '{semantic_model_name}' in workspace '{workspace_name}' fully provisioned and refreshed.")
    return (dataset_name, dataset_id)

def _generate_onelake_shared_expression(
    workspace_id: UUID,
    item_id: UUID,
) -> str:
    """
    Dynamically generates the M expression used by a Direct Lake model in OneLake mode for a given artifact.

    Parameters
    ----------
    workspace_id : UUID
        The ID of the Fabric workspace in which the Fabric item that owns the Delta tables is located.
    item_id : UUID
        The ID of the Fabric item that owns the Delta tables.

    Returns
    -------
    str
        Shows the expression which can be used to connect a Direct Lake semantic model to the DataLake.
    """

    # Get the dfs endpoint of the workspace
    client = fabric.FabricRestClient()
    response = client.get(f"/v1/workspaces/{workspace_id}")
    if response.status_code != 200:
        raise FabricHTTPException(response)

    olEPs = response.json().get("oneLakeEndpoints")
    dfsEP = olEPs.get("dfsEndpoint")
    
    start_expr = "let\n\tdatabase = "
    end_expr = "\nin\n\tdatabase"
    mid_expr = f"AzureStorage.DataLake(\"{dfsEP}/{workspace_id}/{item_id}\")"

    return f"{start_expr}{mid_expr}{end_expr}"

def deprovision_semantic_models(
    test_suite: TestSuite,
    delete_masters: Optional[bool]=False
):
    """
    Deprovisions test clones and optionally also the master semantic models listed in the test definitions.

    Parameters
    ----------
    test_suite : TestSuite
        A TestSuite object with the semantic model and data source definitions, such as the a test suite returned by the _get_test_definitions_df() function.
    delete_masters : bool, default=False
        A flag indicating if master semantic models should be deleted in addition to the test model clones.
    """
    test_definitions = test_suite.to_df()

    masters_df = test_definitions.dropDuplicates(['MasterDataset', 'MasterWorkspace'])
    if delete_masters and masters_df.count() > 0:
        print(f"{icons.in_progress} Deleting '{masters_df.count()}' master semantic models.")
        for m in masters_df.collect():
            # Catch all exceptions so that the loop continues even when an error occurs for a particular model.
            try:
                delete_semantic_model(
                    dataset = m['MasterDataset'],
                    workspace = m['MasterWorkspace']
                )
            except Exception as e:
                print(f"{icons.red_dot} {e}")

    clones_df = test_definitions.dropDuplicates(['TargetDataset', 'TargetWorkspace'])
    if clones_df.count() > 0:
        print(f"{icons.in_progress} Deleting '{clones_df.count()}' test semantic models.")
        for c in clones_df.collect():
            # Catch all exceptions so that the loop continues even when an error occurs for a particular model.
            try:
                delete_semantic_model(
                    dataset = c['TargetDataset'],
                    workspace = c['TargetWorkspace']
                )
            except Exception as e:
                print(f"{icons.red_dot} {e}")
    else:
        print(f"{icons.red_dot} No test semantic models found in the test definitions.")

def delete_semantic_model(
    dataset: str | UUID,
    workspace: str | UUID,
):
    """
    Deletes a semantic model from the specified workspace.

    Parameters
    ----------
    dataset : TestSuite
        The name or ID of the semantic model to be deleted.
    workspace : str | uuid.UUID
        The Fabric workspace name or ID where the semantic model is located.
        The workspace must be specified and must exist or the function fails with a WorkspaceNotFoundException.
    """
    (workspace_name, workspace_id) = resolve_workspace_name_and_id(workspace)
    (dataset_name, dataset_id) = resolve_dataset_name_and_id(dataset=dataset, workspace=workspace_id)

    client = fabric.FabricRestClient()
    response = client.delete(
        f"/v1/workspaces/{workspace_id}/semanticModels/{dataset_id}")

    if response.status_code == 200:
        print(f"{icons.checked} Semantic model '{dataset_name}' in workspace '{workspace_name}' deleted successfully.")
    else:
        print(f"{icons.red_dot} response.")

def provision_test_semantic_models(
    test_suite: TestSuite,
    capacity: Optional[str | UUID] = None,
    refresh_clones: Optional[bool] = True,
    ):
    """
    Creates test models from the master models specified in the test defintions dataframe.

    Parameters
    ----------
    test_suite : TestSuite
        A TestSuite object with the test definitions.
    capacity : str | uuid.UUID, default=None
        The name or ID of the capacity on which to place the new workspace.
        Defaults to None which resolves to the capacity of the workspace of the attached lakehouse
        or if no lakehouse attached, resolves to the capacity of the workspace of the notebook.
    refresh_clones : bool, default=True
        If set to True, this will initiate a full refresh of the cloned semantic models in the target workspace.

    """
    from sempy_labs import deploy_semantic_model
    
    for row in test_suite.to_df().dropDuplicates(['MasterWorkspace','MasterDataset','TargetWorkspace', 'TargetDataset']).collect():
        master_dataset = row['MasterDataset']
        master_workspace = row['MasterWorkspace']
        target_dataset = row['TargetDataset']
        target_workspace = row['TargetWorkspace']

        # Skip this row if master or target are not defined.
        if not master_dataset or not target_dataset:
            continue

        # Make sure the target_workspace exists.
        (target_workspace_name, target_workspace_id) = _get_or_create_workspace(
                workspace=target_workspace, capacity=capacity,
                description="A semantic model for query tests in a perf lab."
            )

        # Do not overwrite existing test models because some model settings, such as Direct Lake autosync
        # can only be configured manually right now, and would be lost if automation just replaced the existing
        # model with a new semantic model clone.
        dfD = fabric.list_datasets(workspace=target_workspace_id, mode="rest")
        dfD_filt = dfD[dfD["Dataset Name"] == target_dataset]
        if dfD_filt.empty:
            deploy_semantic_model(
                source_dataset=master_dataset,
                source_workspace=master_workspace,
                target_dataset=target_dataset,
                target_workspace=target_workspace,
                refresh_target_dataset=refresh_clones,
                overwrite=False,
            )
        else:
            print(f"{icons.green_dot} The test semantic model '{target_dataset}' already exists in the workspace '{target_workspace_name}'.")

            
def _ensure_table(
        table_name: str,
        workspace_id: UUID,
        lakehouse_id: UUID,
        timeout: Optional[int] = 60,
)->bool:
    """
    Checks that a table exists in the specified lakehouse.

    Parameters
    ----------
    table_name : str
        The name of the table to look for.
    workspace_id : uuid.UUID
        The Fabric workspace ID where the lakehouse is located.
    lakehouse_id : uuid.UUID
        The ID of the lakehouse where the delta tables should be added.
    timeout: int, default=30
        The max duration to check for the table.

    Returns
    -------
    bool
        True if the specified table was detected.
        False if the timeout expired before the table was detected.
    """
    import time

    time_in_secs = timeout
    step_size = 5
    while time_in_secs > 0:
        tables_df = get_lakehouse_tables(
            workspace=workspace_id, 
            lakehouse=lakehouse_id
        )

        if tables_df['Table Name'].eq(table_name).any():
            return True
        else:
            time_in_secs -= step_size
            time.sleep(step_size)         
    
    return False
