

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
    import json
    from pprint import pprint
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

    for think in context:
        print(think)
        pprint(context[think])

    payload = {
        "subscription" : "0000-00000-00000-00000",
        "storage_sas" : [
            "https://foobar.blob.core.windows.net/container/folder/firstfile.ppt?SAS_TOKEN_HERE",
            "https://foobar.blob.core.windows.net/container/folder/secondfile.ppt?SAS_TOKEN_HERE"
        ]
    }

    return json.dumps(payload)


