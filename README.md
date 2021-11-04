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

# Notes

## PythonVirtualEnvOperator
It appears that this virtual environment does not get access to the main task "params" field from the context as the standard PythonOperators do. 

That is, you have to pass everything into it from the definition file. 

Now, I'm not entirely sure we NEED to have any virtual envrironment tasks to begin with but I'm trying to figure out how to make this work. 

## Loaded configuration
As I worked through this, I came up with a pseudo solution in that certain configuration can be maintained in a local JSON file to the DAG.

Other configuration comes from the DAG execution through a config, so

### PythonOperator
- Access to the settings from the configuration json file
- Access to the configuration object passed in the dag context as 'params'

### PythonVirtualEnvOperator
- Access to the settings from the configuration json file ONLY

