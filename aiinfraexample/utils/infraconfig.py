"""
Class used to parse the parameters from a context object from 
Airflow to teh DAG
"""
import os
import json

class ConfigurationConstants:
    XCOM_TARGET = "xcom_target"
    TASK_INSTANCE = "task_instance"
    TASK_PARAMS = "params"
    DEPLOYMENT_SETTINGS = "deployment_info"


class Configuration:
    """
    Generic configuraton object that loads the "params" from the 
    task context sent to non virtualenv python tasks. 
    """
    def __init__(self, context):
        if ConfigurationConstants.TASK_PARAMS in context:
            for param in context[ConfigurationConstants.TASK_PARAMS]:
                setattr(self, param, context[ConfigurationConstants.TASK_PARAMS][param])


class DeploymentConfiguration:
    def __init__(self, directory:str, config_file:str):
        self.directory = directory
        self.config_file = config_file
        self.config_object = None

        path = os.path.join(self.directory, self.config_file)
        if os.path.exists(path):
            with open(path, "r") as settings:
                content = settings.readlines()
                content = "\n".join(content)
                self.config_object = json.loads(content)

    def get_config(self, optionals:dict = None):
        return_data = {ConfigurationConstants.DEPLOYMENT_SETTINGS : None}
        if self.config_object:
            return_data[ConfigurationConstants.DEPLOYMENT_SETTINGS] = self.config_object.copy()
            if optionals and isinstance(optionals, dict):
                return_data.update(optionals)

        return return_data