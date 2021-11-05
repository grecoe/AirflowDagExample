

def virtualenv_endpoint(*args, **context):
    """
    Example function that will be performed in a virtual environment.

    https://github.com/apache/airflow/blob/566127308f283e2eff29e8a7fbfb01f17a1cd18a/airflow/operators/python.py#L218-L231

    "The function must be defined using def, and not be
    part of a class. All imports must happen inside the function
    and no variables outside of the scope may be referenced."

    Because this operator has no access to the DAG execution parameters, they are read
    from disk after being placed there by the first DAG task. 

    """
    import os
    import json
    from pprint import pprint
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

    """
    Because we have no access to the ConfigurationConstants class, we have to duplicate the keys
    here for what we are looking for in the deployment information. 
    """
    DEPLOYMENT_SETTINGS = "deployment_info"
    DEPLOYMENT_PARAMS_FILE = "params_file"
    DEPLOYMENT_PARAMS_DIRECTORY = "params_folder"

    deployment_settings = None
    deployment_config = None

    """
    First, see if the context (extended kwargs) has the deployment information
    """
    print("Deployment settings from JSON?")
    if DEPLOYMENT_SETTINGS in context:
        deployment_settings = context[DEPLOYMENT_SETTINGS]

    """
    Second, see if the deployment information has information about where the execution
    parameters are stored. If found, load it.

    This is important because our task will likely need specific information about what to load.
    """
    if DEPLOYMENT_PARAMS_FILE in deployment_settings and DEPLOYMENT_PARAMS_DIRECTORY in deployment_settings:
        file_path = os.path.join(deployment_settings[DEPLOYMENT_PARAMS_DIRECTORY], deployment_settings[DEPLOYMENT_PARAMS_FILE])
        if os.path.exists(file_path):
            with open(file_path, "r") as context_data:
                content = context_data.readlines()
                content = "\n".join(content)
                deployment_config = json.loads(content)

    
    print("Virtual Environment")
    print("Deploymnet settings")
    pprint(deployment_settings)
    print("Execution context")
    pprint(deployment_config)


    """
    Create a mock return value as we have not actually done any real work. This information will be 
    passed via xcom so that downstream tasks can work on the items. 
    """
    payload = {
        "subscription" : "0000-00000-00000-00000",
        "storage_sas" : [
            "https://foobar.blob.core.windows.net/container/folder/firstfile.ppt?SAS_TOKEN_HERE",
            "https://foobar.blob.core.windows.net/container/folder/secondfile.ppt?SAS_TOKEN_HERE"
        ]
    }

    return json.dumps(payload)


