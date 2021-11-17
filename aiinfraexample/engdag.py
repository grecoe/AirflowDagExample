"""
Example DAG showing PythonOperator and PythonVirtualenvOperators 
- Declaration
- Using a sideloaded configuration
- Using incoming configuration
- Passing data with xcom
"""

import logging
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator

from aiinfraexample.utils import (
    Tasks,
    SideLoadConfiguration,
    ConfigurationConstants,
    virtualenv_endpoint, 
    az_cli_data_collection
)
# Cannot be in the above init file or it hoses the virtual environment thigs.
from aiinfraexample.utils.task.storagetask import StorageTask
# Have to be very thoughtful about how to bring in documents, particularly here in this file.

SIDELOAD_CONFIGURATION_FILE = "exampleconf.json"


def dummy_task(**context):
    return {ConfigurationConstants.UPLOAD_TARGET : os.path.join(os.path.split(__file__)[0], SIDELOAD_CONFIGURATION_FILE)}

log = logging.getLogger(__name__)

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


    # Get teh file locally that we need
    retrieve_file_task = PythonOperator(
        task_id='0_download_blob',
        python_callable=StorageTask.download_file_task,
        op_kwargs= sideload_config.get_config()
    )  

    # Upload the file to storage
    settings = sideload_config.get_config(
        {
            ConfigurationConstants.XCOM_TARGET : "0_download_blob"
        }
    )
    upload_file_task = PythonOperator(
        task_id='1_push_blob',
        python_callable=StorageTask.upload_file_task,
        op_kwargs= settings
    )  
    # TESTING



    """
    Collect what you need from Azure CLI commands not present in 
    other Python libraries. This can be grabbed from x_com as needed
    except in another Virtual Env Operator.

    Because it sets the path to the DAG folder itself, we have to include whatever 
    is being referenced in there. 
    cli_data_collection = PythonVirtualenvOperator(
        task_id="a_cli_data_collection",
        python_callable=az_cli_data_collection,
        requirements=[
            "azure-cli"           
            ],
        system_site_packages=False,
        op_kwargs= sideload_config.get_config(),
        dag=dag
    )
    """

    """
    Persist the  execution configuration (conext["params"]) to the file system becasue 
    the PythonVirtualenvOperator does not currently recieve them through the context object 
    it recieves. 

    We also can extend that with any other outputs from previous tasks since a virtual env
    operator also does not have access to the main context object either. 
    next_settings = sideload_config.get_config(
        {
            ConfigurationConstants.XCOM_TARGET : "a_cli_data_collection"
        }
    )
    persist_context_params_step = PythonOperator(
        task_id='b_persist_context',
        python_callable=Tasks.persist_context_params,
        op_kwargs= next_settings
    )  
    """

    """
    The PythonVirtualenvOperator allows you to provde requirements for the environment. These
    are things not installed by default in the Airflow environment. 
    scan_storage_step = PythonVirtualenvOperator(
        task_id="c_storage_scan",
        python_callable=virtualenv_endpoint,
        requirements=[
            "azure-storage-blob",
            "azure-identity==1.5.0"
            ],
        system_site_packages=False,
        op_kwargs= sideload_config.get_config(),
        dag=dag
    )
    """

    """
    Create a PythonOperator and pass along the DeploymentConfiguration (json content), 
    but extend it to identify it also needs the output of the previous task. This will
    be loaded appropriately by the BaseTask class when this step executes.
    next_settings = sideload_config.get_config(
        {
            ConfigurationConstants.XCOM_TARGET : ["c_storage_scan", "a_cli_data_collection"]
        }
    )
    process_step = PythonOperator(
        task_id='d_process_storage',
        python_callable=Tasks.process_storage,
        op_kwargs= next_settings
    )    
    """ 

    """
    Create a PythonOperator and pass along the DeploymentConfiguration (json content), 
    but extend it to identify it also needs the output of the previous task. This will
    be loaded appropriately by the BaseTask class when this step executes.
    next_settings = sideload_config.get_config(
        {
            ConfigurationConstants.XCOM_TARGET : "d_process_storage"
        }
    )
    store_step = PythonOperator(
        task_id='e_store_results',
        python_callable=Tasks.store_results,
        op_kwargs= next_settings
    )    
    """ 

    """
    Now set the execution flow of the DAG
    """
    #dummy_dag_task >> test_storage_task >> cli_data_collection >> persist_context_params_step >> scan_storage_step >> process_step >> store_step
    retrieve_file_task >> upload_file_task
