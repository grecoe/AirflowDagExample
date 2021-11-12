import logging
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator

from aiinfraexample import ProjectConfiguration
from aiinfraexample.utils.basetasks import Tasks
from aiinfraexample.utils.infraconfig import DeploymentConfiguration, ConfigurationConstants
from aiinfraexample.utils.virtenvtask import virtualenv_endpoint
from aiinfraexample.utils.azureclitask import az_cli_data_collection


log = logging.getLogger(__name__)

with DAG(
    dag_id='aiinfra_example',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['datapassing'],
    params={"ai_infra_project": "initial_test"},
) as dag:

    """
    Load the overall configuration JSON that will be passed along to each of the tasks 
    through the op_kwargs field which will extend the context dictionary it recieves. 
    """
    deploy_config = DeploymentConfiguration(
        os.path.split(__file__)[0], 
        ProjectConfiguration.CONFIGURATION_FILE
    )

    deploy_config.update_config(
        ConfigurationConstants.DEPLOYMENT_PARAMS_DIRECTORY,
        os.path.split(__file__)[0]
        )


    """
    Collect what you need from Azure CLI commands not present in 
    other Python libraries. This can be grabbed from x_com as needed
    except in another Virtual Env Operator.
    """
    cli_data_collection = PythonVirtualenvOperator(
        task_id="cli_data_collection",
        python_callable=az_cli_data_collection,
        requirements=[
            "azure-cli"
            ],
        system_site_packages=False,
        op_kwargs= deploy_config.get_config(),
        dag=dag
    )

    """
    Persist the  execution configuration (conext["params"]) to the file system becasue 
    the PythonVirtualenvOperator does not currently recieve them through the context object 
    it recieves. 
    """
    persist_context_params_step = PythonOperator(
        task_id='a_persist_context',
        python_callable=Tasks.persist_context_params,
        op_kwargs= deploy_config.get_config()
    )  

    """
    The PythonVirtualenvOperator allows you to provde requirements for the environment. These
    are things not installed by default in the Airflow environment. 
    """
    scan_storage_step = PythonVirtualenvOperator(
        task_id="b_storage_scan",
        python_callable=virtualenv_endpoint,
        requirements=[
            "azure-storage-blob==12.9.0",
            "azure-identity==1.7.0"
            ],
        system_site_packages=False,
        op_kwargs= deploy_config.get_config(),
        dag=dag
    )

    """
    Create a PythonOperator and pass along the DeploymentConfiguration (json content), 
    but extend it to identify it also needs the output of the previous task. This will
    be loaded appropriately by the BaseTask class when this step executes.
    """ 
    next_settings = deploy_config.get_config(
        {
            ConfigurationConstants.XCOM_TARGET : ["b_storage_scan", "cli_data_collection"]
        }
    )
    process_step = PythonOperator(
        task_id='c_process_storage',
        python_callable=Tasks.process_storage,
        op_kwargs= next_settings
    )    

    """
    Create a PythonOperator and pass along the DeploymentConfiguration (json content), 
    but extend it to identify it also needs the output of the previous task. This will
    be loaded appropriately by the BaseTask class when this step executes.
    """ 
    next_settings = deploy_config.get_config(
        {
            ConfigurationConstants.XCOM_TARGET : "c_process_storage"
        }
    )
    store_step = PythonOperator(
        task_id='d_store_results',
        python_callable=Tasks.store_results,
        op_kwargs= next_settings
    )    

    """
    Now set the execution flow of the DAG
    """
    cli_data_collection >> persist_context_params_step >> scan_storage_step >> process_step >> store_step
