"""
Class used to parse the parameters from a context object from 
Airflow to teh DAG
"""
import os
import json

class ConfigurationConstants:
    """
    Constant configuration keys so that all tasks/process are talking the same
    language to parse objects.
    """
    # START
    # Sideloaded settings from JSON 
    SIDELOAD_SETTINGS = "sideload_info"
    PACKAGE_HOME = "package_home"
    # Sideloaded fields expected to be there
    DEPLOYMENT_SUBSCRIPTION = "subscription"
    COGSRCH_API_VERSION = "cog_search_api_version"
    COGSRCH_INSTANCE = "cog_search"
    COGSRCH_INSTANCE_RG = "cog_search_rg"
    COGSRCH_INDEXER = "cog_search_indexer"
    COGSRCH_INDEX = "cog_search_index"
    COGSRCH_DATA_SRC = "cog_search_datasource"

    # File used to pass context parameters to virtual env operators
    DEPLOYMENT_PARAMS_FILE = "params_file"
    DEPLOYMENT_PARAMS_DIRECTORY = "params_folder"
    # END

    # XCOM Related fields added to settings inbound
    XCOM_TARGET = "xcom_target"
    TASK_INSTANCE = "task_instance"
    TASK_PARAMS = "params"

    # XCOM data passed
    UPLOAD_TARGET = "upload_target"
    COGSRCH_ADMIN_KEY = "cog_search_key"
    COGSRCH_DATA_SRC_ACC = "cog_search_datasource_account"
    COGSRCH_DATA_SRC_CTR = "cog_search_datasource_container"
    COGSRCH_DATA_SRC_FLD = "cog_search_datasource_folder"
    COGSRCH_INDEXER_PROC_CNT = "cog_search_indexer_proc_count"
    COGSRCH_RESULT_CONTENT = "cog_search_result_record"

    # Execution Config Parameters
    SYSTEM_FILE_ID = "file_id" # Testing is blob name
    SYSTEM_PARTITION_ID = "partition_id" # Testiong is container
    SYSTEM_FILE_KIND = "kind" # Tsting is account


class AirflowContextConfiguration:
    """
    Generic configuraton object that loads the "params" from the 
    task context sent to non virtualenv python tasks. 
    """
    def __init__(self, context):
        if ConfigurationConstants.TASK_PARAMS in context:
            for param in context[ConfigurationConstants.TASK_PARAMS]:
                setattr(self, param, context[ConfigurationConstants.TASK_PARAMS][param])

    def has_attribute(self, attribute_name):
        return attribute_name in self.__dict__

    def get_attribute(self, attribute_name):
        return self.__dict__[attribute_name]

    def to_json(self, additional:dict = None):
        return_value = None

        output = self.__dict__.copy()
        if additional:
            output.update(additional)

        if len(output):
            return_value = json.dumps(output)
        return return_value

class SideLoadConfiguration:
    """
    Wrapper for whatever JSON configuration file is going to be used across tasks/stages.
    """
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

    def update_config(self, field_name, field_value):
        if self.config_object and isinstance(self.config_object, dict):
            self.config_object[field_name] = field_value

    def get_config(self, optionals:dict = None):
        return_data = {ConfigurationConstants.SIDELOAD_SETTINGS : None}
        if self.config_object:
            return_data[ConfigurationConstants.SIDELOAD_SETTINGS] = self.config_object.copy()
            if optionals and isinstance(optionals, dict):
                return_data.update(optionals)

        return return_data