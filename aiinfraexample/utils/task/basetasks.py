
import json
import os
from pprint import pprint
from aiinfraexample.utils import (
    AirflowContextConfiguration, 
    ConfigurationConstants
)

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
        self.configuration = AirflowContextConfiguration(context)
        self.sideload_settings = None
        self.xcom_target = {}

        if ConfigurationConstants.TASK_INSTANCE in context and ConfigurationConstants.XCOM_TARGET in context:
            targets = context[ConfigurationConstants.XCOM_TARGET]
            if not isinstance(targets, list):
                targets = [targets]

            for target in targets:
                self.xcom_target[target] = context[ConfigurationConstants.TASK_INSTANCE].xcom_pull(
                    task_ids=target
                    )

            if len(self.xcom_target):
                for target in self.xcom_target:
                    try:
                        self.xcom_target[target] = json.loads(self.xcom_target[target])
                    except:
                        # Not a JSON object
                        pass

        if ConfigurationConstants.SIDELOAD_SETTINGS in context:
            self.sideload_settings = context[ConfigurationConstants.SIDELOAD_SETTINGS]
            if isinstance(self.sideload_settings, str):
                self.sideload_settings = json.loads(self.sideload_settings)

    def find_xcom_target(self, field_name: str):
        return_value = None
        if len(self.xcom_target):
            for target in self.xcom_target:
                if isinstance(self.xcom_target[target], dict):
                    if field_name in self.xcom_target[target]:
                        return_value = self.xcom_target[target][field_name]
                        break
        return return_value

    def summarize(self):
        print("Execution Configuration from DAG:")
        pprint( self.configuration.to_json())

        print("Side Load Settings from JSON:")
        pprint( self.sideload_settings)

        print("XCOM Passed Data:")
        pprint(self.xcom_target)

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
        base_task.summarize()

        xcom_additions = None
        if len(base_task.xcom_target):
            xcom_additions = {}
            for target in base_task.xcom_target:
                xcom_additions[target] = base_task.xcom_target[target]

        context_params = base_task.configuration.to_json(xcom_additions)

        written = False
        if context_params:
            if ConfigurationConstants.DEPLOYMENT_PARAMS_FILE in base_task.sideload_settings and \
               ConfigurationConstants.DEPLOYMENT_PARAMS_DIRECTORY in base_task.sideload_settings:
                path = os.path.join(
                    base_task.sideload_settings[ConfigurationConstants.DEPLOYMENT_PARAMS_DIRECTORY],
                    base_task.sideload_settings[ConfigurationConstants.DEPLOYMENT_PARAMS_FILE],
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
        base_task.summarize()

        return_value = base_task.find_xcom_target("storage_sas")

        """
        if len(base_task.xcom_target):
            # Expect to get a storage_sas from one of the x_com targets
            for target in base_task.xcom_target:
                if "storage_sas" in base_task.xcom_target[target]:
                    return_value = json.dumps(base_task.xcom_target[target]["storage_sas"])
        """

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
        base_task.summarize()

        """
        print("Configuration from DAG:")
        pprint( base_task.configuration.to_json())

        print("Side Load Settings from JSON:")
        pprint( base_task.sideload_settings)

        if len(base_task.xcom_target):
            print("XCOM Target = ", json.dumps(base_task.xcom_target))
        """

        """
        Returns nothing because this the end of the line...
        """
