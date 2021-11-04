from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
import logging
import json 
import os

from aiinfraexample.utils.basetasks import Tasks
from aiinfraexample.utils.infraconfig import DeploymentConfiguration, ConfigurationConstants
from aiinfraexample.utils.virtenvtask import virtualenv_endpoint

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

    deploy_config = DeploymentConfiguration(os.path.split(__file__)[0], "exampleconf.json") 

    scan_storage_step = PythonVirtualenvOperator(
        task_id="a_storage_scan",
        python_callable=virtualenv_endpoint,
        requirements=[
            "azure-storage-blob==12.9.0",
            "azure-identity==1.7.0"
            ],
        system_site_packages=False,
        op_kwargs= deploy_config.get_config(),
        dag=dag
    )

    next_settings = deploy_config.get_config(
        {
            ConfigurationConstants.XCOM_TARGET : "a_storage_scan"
        }
    )
    process_step = PythonOperator(
        task_id='b_process_storage',
        python_callable=Tasks.process_storage,
        op_kwargs= next_settings
    )    

    next_settings = deploy_config.get_config(
        {
            ConfigurationConstants.XCOM_TARGET : "b_process_storage"
        }
    )
    store_step = PythonOperator(
        task_id='c_store_results',
        python_callable=Tasks.store_results,
        op_kwargs= next_settings
    )    

    scan_storage_step >> process_step >> store_step
