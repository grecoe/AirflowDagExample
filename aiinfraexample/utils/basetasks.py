
import json
import os
from pprint import pprint
from aiinfraexample.utils.infraconfig import Configuration, ConfigurationConstants

class BaseTask:
    """
    Class that is provided the context object of the DAG execution. With that information 
    it is parsed to give straighforward access to whatever the task requires. 

    - The raw Airflow context object (dict)
    - Parsed out execution configuration
    - Parsed out deployment information (content of exampleconf.json) if present
    - The xcom result to be used (identified in the context with the ConfigurationConstants.XCOM_TARGET key)
    """

    def __init__(self, context):
        self.context = context
        self.configuration = Configuration(context)
        self.deployment_settings = None
        self.xcom_target = None
        self.xcom_result = None

        if ConfigurationConstants.TASK_INSTANCE in context and ConfigurationConstants.XCOM_TARGET in context:
            self.xcom_target = context[ConfigurationConstants.XCOM_TARGET]
            self.xcom_result = context[ConfigurationConstants.TASK_INSTANCE].xcom_pull(
                task_ids=context[ConfigurationConstants.XCOM_TARGET]
                )

            if self.xcom_result:
                try:
                    self.xcom_result = json.loads(self.xcom_result)
                except:
                    # Not a JSON object
                    pass

        if ConfigurationConstants.DEPLOYMENT_SETTINGS in context:
            self.deployment_settings = context[ConfigurationConstants.DEPLOYMENT_SETTINGS]
            if isinstance(self.deployment_settings, str):
                self.deployment_settings = json.loads(self.deployment_settings)


class Tasks:

    @staticmethod
    def persist_context_params(**context):
        """
        Task that persists the execution configuration (params) from the DAG execution to the
        file system in a predetermined location.

        This is done so that the PythonVirtualenvOperator will have access to those settings.
        """
        print("Persist context params for PythonVirtualEnvOperator")

        """
        Create a BaseTask object that will parse the context and provide simple access to
        - The deployment information from the JSON file
        - The execution parameters passed to the DAG
        - The data passed from the previous task via xcom
        """
        base_task = BaseTask(context)

        context_params = base_task.configuration.to_json()

        written = False
        if context_params:
            if ConfigurationConstants.DEPLOYMENT_PARAMS_FILE in base_task.deployment_settings and \
               ConfigurationConstants.DEPLOYMENT_PARAMS_DIRECTORY in base_task.deployment_settings:
                path = os.path.join(
                    base_task.deployment_settings[ConfigurationConstants.DEPLOYMENT_PARAMS_DIRECTORY],
                    base_task.deployment_settings[ConfigurationConstants.DEPLOYMENT_PARAMS_FILE],
                )
                with open(path, "w") as persisted_context:
                    persisted_context.writelines(context_params)
                
                written = True

        print("Context parameters have been written:", written)


    @staticmethod
    def process_storage(**context):
        """
        Mock tasks that just verifies it has access to the settings ard returns some value
        for the next task. 
        """
        return_value = None

        print("In process storage")

        """
        Create a BaseTask object that will parse the context and provide simple access to
        - The deployment information from the JSON file
        - The execution parameters passed to the DAG
        - The data passed from the previous task via xcom
        """
        base_task = BaseTask(context)
        print("Configuration from DAG:")
        pprint( base_task.configuration.to_json())

        print("Deployment Settings from JSON:")
        pprint( base_task.deployment_settings)

        if base_task.xcom_target and base_task.xcom_result:
            print("XCOM Target = ", base_task.xcom_target)

            print(json.dumps(base_task.xcom_result))
            return_value = json.dumps(base_task.xcom_result["storage_sas"])
        
        """
        Return some data to be passed via xcom so that downstream tasks can work on the items. 
        """
        return return_value

    @staticmethod
    def store_results(**context):
        """
        Mock tasks that just verifies it has access to the settings. 
        """
        print("In store results")

        """
        Create a BaseTask object that will parse the context and provide simple access to
        - The deployment information from the JSON file
        - The execution parameters passed to the DAG
        - The data passed from the previous task via xcom
        """
        base_task = BaseTask(context)
        print("Configuration from DAG:")
        pprint( base_task.configuration.to_json())

        print("Deployment Settings from JSON:")
        pprint( base_task.deployment_settings)

        if base_task.xcom_target and base_task.xcom_result:
            print("XCOM Target = ", base_task.xcom_target)

            print(json.dumps(base_task.xcom_result))

        """
        Returns nothing because this the end of the line...
        """
