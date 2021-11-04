

def virtualenv_endpoint(*args, **context):
    """
    Example function that will be performed in a virtual environment.

    https://github.com/apache/airflow/blob/566127308f283e2eff29e8a7fbfb01f17a1cd18a/airflow/operators/python.py#L218-L231

    "The function must be defined using def, and not be
    part of a class. All imports must happen inside the function
    and no variables outside of the scope may be referenced."

    Importing at the module level ensures that it will not attempt to import the
    library before it is installed.

    I cannot for the life of me figure out how to get teh context config/params to come into 
    a virtual env....so this might be a pointless exercise.
    """
    import os
    import json
    from pprint import pprint
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

    DEPLOYMENT_SETTINGS = "deployment_info"
    DEPLOYMENT_PARAMS_FILE = "params_file"
    DEPLOYMENT_PARAMS_DIRECTORY = "params_folder"

    """
    This doesn't tell us much...other than the conext does not contain the original config
    settings passed to the dag....but we do have a couple of things we can use. 
    """
    """
    print("Context:")
    for thing in context:
        print(thing)
        pprint(context[thing])
    """

    deployment_settings = None
    deployment_config = None

    print("Deployment settings from JSON?")
    if DEPLOYMENT_SETTINGS in context:
        deployment_settings = context[DEPLOYMENT_SETTINGS]

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



    payload = {
        "subscription" : "0000-00000-00000-00000",
        "storage_sas" : [
            "https://foobar.blob.core.windows.net/container/folder/firstfile.ppt?SAS_TOKEN_HERE",
            "https://foobar.blob.core.windows.net/container/folder/secondfile.ppt?SAS_TOKEN_HERE"
        ]
    }

    return json.dumps(payload)


