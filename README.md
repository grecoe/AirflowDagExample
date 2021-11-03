# AirflowDagExample

Example on how to use Airflow DAG on a Virtual Machine. 

1. Create a Virtual Machine in Azure (I chose the Ubuntu DSVM because it has a bunch of needed tools already installed)
2. Create a conda environment with this environment file:
```
name: AirflowEnv
channels:
  - conda-forge
dependencies:
  - python=3.9.2
  - pip==21.2.4
  - virtualenv
```
3. Activate the environment
> conda activate AirflowEnv
4. Follow all of the [Airflow run local](https://airflow.apache.org/docs/apache-airflow/stable/start/local.html)
    - Ensure you run airflow standalone at least once to seed it. 
5. Drop the aiinfraexample folder into the dag folder. You can find this by running the command
> airflow info
```
Apache Airflow
version                | 2.2.1                                              
...
--> dags_folder            | /home/YOURUSER/airflow/dags                          
...
```
6. Restart airflow (airflow standalone)

> NOTE: When airflow is running you will not be able to create a file or folder under the dags folder. Simply shut it down and then create what you need. 

# DAG Files
Look under aiinfraexample/exampledag.py, there are three tasks in there:

1. A PythonVirtualEnvOperator that I'm still trying to figure out. 
2. Two other PythonOperator that are just chained in there and use xcom to pass data from one to the next


