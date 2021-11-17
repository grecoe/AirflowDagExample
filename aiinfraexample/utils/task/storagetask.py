import os
from aiinfraexample.utils import ConfigurationConstants
from aiinfraexample.utils.task.basetasks import BaseTask
from aiinfraexample.utils.storage.storageutil import AzureStorageUtil
from pprint import pprint
from azure.identity import (
    DefaultAzureCredential,
)

class StorageTask:

    @staticmethod
    def download_file_task(**context):
        print("In download file")

        """
        Create a BaseTask object that will parse the context and provide simple access to
        - The deployment information from the JSON file
        - The execution parameters passed to the DAG
        - The data passed from the previous task via xcom
        """
        base_task = BaseTask(context)
        base_task.summarize()

        # Verify sideloaded settings
        if not base_task.configuration.has_attribute(ConfigurationConstants.SYSTEM_FILE_ID) or \
            not base_task.configuration.has_attribute(ConfigurationConstants.SYSTEM_PARTITION_ID) or \
            not base_task.configuration.has_attribute(ConfigurationConstants.SYSTEM_FILE_KIND) or \
            ConfigurationConstants.DEPLOYMENT_PARAMS_DIRECTORY not in base_task.sideload_settings :
            raise Exception("Data source information is incomplete")
        
        blob_name = base_task.configuration.get_attribute(ConfigurationConstants.SYSTEM_FILE_ID)
        container = base_task.configuration.get_attribute(ConfigurationConstants.SYSTEM_PARTITION_ID) 
        account = base_task.configuration.get_attribute(ConfigurationConstants.SYSTEM_FILE_KIND)

        # Location for the local download
        local_file = os.path.join(
            base_task.sideload_settings[ConfigurationConstants.DEPLOYMENT_PARAMS_DIRECTORY],
            os.path.split(blob_name)[1]
        )

        az_stg_utils = AzureStorageUtil(
            account,
            DefaultAzureCredential()
        )

        print("Blob", blob_name, "downloaded to", local_file)

        az_stg_utils.download_blob(container, blob_name, local_file)
        
        return {ConfigurationConstants.UPLOAD_TARGET : local_file}

    @staticmethod
    def upload_file_task(**context):
        print("In upload file")

        """
        Create a BaseTask object that will parse the context and provide simple access to
        - The deployment information from the JSON file
        - The execution parameters passed to the DAG
        - The data passed from the previous task via xcom
        """
        base_task = BaseTask(context)
        base_task.summarize()

        
        # Validate we actually got a target
        local_file = base_task.find_xcom_target(ConfigurationConstants.UPLOAD_TARGET)
        storage_account = base_task.find_xcom_target(ConfigurationConstants.COGSRCH_DATA_SRC_ACC)
        storage_folder = base_task.find_xcom_target(ConfigurationConstants.COGSRCH_DATA_SRC_FLD)
        storage_container = base_task.find_xcom_target(ConfigurationConstants.COGSRCH_DATA_SRC_CTR)

        if not local_file or not storage_account or not storage_container:
            raise Exception("Do not have an upload target, or missing storage information")

        file_name = os.path.split(local_file)[1]
        blob_path = file_name
        if storage_folder:
            if storage_folder[-1] != '/':
                storage_folder += '/'

            blob_path = "{}{}".format(
                storage_folder,
                file_name
            )


        az_stg_utils = AzureStorageUtil(
            storage_account,
            DefaultAzureCredential()
        )

        # Delete if there (and create container if needed)
        print("Delete blob", blob_path)
        az_stg_utils.delete_process_file(
            storage_container,
            blob_path
        )

        # Upload what we have
        print("Uploading ", local_file)
        az_stg_utils.upload_process_file(
            storage_container,
            blob_path,
            local_file
        )

        print("Removing local file")
        os.remove(local_file)

        print("Done")