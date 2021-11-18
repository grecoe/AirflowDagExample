"""
A more robust example using Azure Cognitive Search and Azure Storage
to process records then retrieve the results. 

Configuration of that environment is up to the reader. 
"""

import os
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator

from aiinfraexample.utils import (
    SideLoadConfiguration,
    ConfigurationConstants,
    az_cli_data_collection
)
# Cannot be in the above init file or it hoses the virtual environment thigs.
from aiinfraexample.utils.task.storagetask import StorageTask

from aiinfraexample.utils.task.cogsearchtask import CogSearchTask
from aiinfraexample.utils.task.wkstask import WellKnownSchemaTask

def mock_key_retrieval(**context):
    return {ConfigurationConstants.COGSRCH_ADMIN_KEY : "YOUR_COG_SRCH_ADMIN_KEY"}

log = logging.getLogger(__name__)

SIDELOAD_CONFIGURATION_FILE = "exampleconf.json"

with DAG(
    dag_id='aieng_example',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['dataprocessing'],
    params={
        "project": "test"
        },
) as dag:

    """
    Load the overall configuration JSON that will be passed along to each of the tasks 
    through the op_kwargs field which will extend the context dictionary it recieves. 
    """
    sideload_config = SideLoadConfiguration(
        os.path.split(__file__)[0], 
        SIDELOAD_CONFIGURATION_FILE
    )

    # Update where teh deployment parameters configuration file is
    sideload_config.update_config(
        ConfigurationConstants.DEPLOYMENT_PARAMS_DIRECTORY,
        os.path.split(__file__)[0]
        )
    # Update where the package home is so virtualenv can load from this
    # package.
    dag_location = os.path.split(__file__)[0]
    dag_home = os.path.split(dag_location)[0]
    sideload_config.update_config(
        ConfigurationConstants.PACKAGE_HOME,
        dag_home
        )

    # Get the key from cog search (use mock for now to save dev time)
    get_search_key = PythonOperator(
        task_id='0_get_search_key',
        python_callable=mock_key_retrieval,
        op_kwargs= sideload_config.get_config()
    )  
    """
    This step is pretty slow so using a mock above (replace with your admin key)
    but when ready to go "live" this will get the admin key from cognitive search.

    get_search_key = PythonVirtualenvOperator(
        task_id="0_get_search_key",
        python_callable=az_cli_data_collection,
        requirements=[
            "azure-cli"           
            ],
        system_site_packages=False,
        op_kwargs= sideload_config.get_config()
    )
    """    

    # Get the storage info from cog search data source
    settings = sideload_config.get_config(
        {
            ConfigurationConstants.XCOM_TARGET : "0_get_search_key"
        }
    )
    get_datastore_details_task = PythonOperator(
        task_id='1_get_datastore_info',
        python_callable=CogSearchTask.get_datasource_details,
        op_kwargs= settings
    )  

    # Get teh file locally that we need
    retrieve_file_task = PythonOperator(
        task_id='2_download_blob',
        python_callable=StorageTask.download_file_task,
        op_kwargs= sideload_config.get_config()
    )  

    # Upload the file to storage
    settings = sideload_config.get_config(
        {
            ConfigurationConstants.XCOM_TARGET : [
                "1_get_datastore_info",
                "2_download_blob",
            ]
        }
    )
    upload_file_task = PythonOperator(
        task_id='3_push_blob',
        python_callable=StorageTask.upload_file_task,
        op_kwargs= settings
    )  

    # Trigger the indexer
    settings = sideload_config.get_config(
        {
            ConfigurationConstants.XCOM_TARGET : "0_get_search_key"
        }
    )
    trigger_indexer_task = PythonOperator(
        task_id='4_trigger_indexer',
        python_callable=CogSearchTask.trigger_indexer,
        op_kwargs= settings
    )  

    # Trigger the indexer
    settings = sideload_config.get_config(
        {
            ConfigurationConstants.XCOM_TARGET : "0_get_search_key"
        }
    )
    search_index_task = PythonOperator(
        task_id='5_search_index',
        python_callable=CogSearchTask.retrieve_results,
        op_kwargs= settings
    )  

    # Trigger the Well Known Schema massager
    settings = sideload_config.get_config(
        {
            ConfigurationConstants.XCOM_TARGET : "5_search_index"
        }
    )
    wks_task = PythonOperator(
        task_id='6_schema_translate',
        python_callable=WellKnownSchemaTask.massage_processed_record,
        op_kwargs= settings
    )  



    """
    Now set the execution flow of the DAG
    """
    #dummy_dag_task >> test_storage_task >> cli_data_collection >> persist_context_params_step >> scan_storage_step >> process_step >> store_step
    get_search_key >> get_datastore_details_task >> retrieve_file_task >> upload_file_task >> trigger_indexer_task >> search_index_task >> wks_task
