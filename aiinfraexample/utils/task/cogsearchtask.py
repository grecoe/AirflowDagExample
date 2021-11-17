from aiinfraexample.utils import ConfigurationConstants
from aiinfraexample.utils.task.basetasks import BaseTask
from aiinfraexample.utils.search.cogsrchutil import CogSearchDataSourceUtils, CogSearchIndexerUtils
from pprint import pprint

class CogSearchTask:

    @staticmethod
    def get_datasource_details(**context):
        print("In datasource details")

        """
        Create a BaseTask object that will parse the context and provide simple access to
        - The deployment information from the JSON file
        - The execution parameters passed to the DAG
        - The data passed from the previous task via xcom
        """
        base_task = BaseTask(context)
        base_task.summarize()

        api_key = base_task.find_xcom_target(ConfigurationConstants.COGSRCH_ADMIN_KEY)

        if not api_key or \
            ConfigurationConstants.COGSRCH_API_VERSION not in base_task.sideload_settings or \
            ConfigurationConstants.COGSRCH_INSTANCE not in base_task.sideload_settings or \
            ConfigurationConstants.COGSRCH_DATA_SRC not in base_task.sideload_settings:
            raise Exception("Data source information is incomplete")
        
        api_version = base_task.sideload_settings[ConfigurationConstants.COGSRCH_API_VERSION]
        cog_search_inst = base_task.sideload_settings[ConfigurationConstants.COGSRCH_INSTANCE] 
        cog_search_datasource  = base_task.sideload_settings[ConfigurationConstants.COGSRCH_DATA_SRC]

        print("Key", api_key)
        print(api_version)
        print(cog_search_inst)
        print(cog_search_datasource)

        datasource_utils = CogSearchDataSourceUtils(cog_search_inst, cog_search_datasource)
        details = datasource_utils.get_datasource_details(api_key, api_version)

        return_value = {}
        if details:
            return_value[ConfigurationConstants.COGSRCH_DATA_SRC_ACC] = details.account_name
            return_value[ConfigurationConstants.COGSRCH_DATA_SRC_CTR] = details.container_name
            return_value[ConfigurationConstants.COGSRCH_DATA_SRC_FLD] = details.folder_path

        return return_value

    @staticmethod
    def trigger_indexer(**context):
        print("In datasource trigger indexer")

        """
        Create a BaseTask object that will parse the context and provide simple access to
        - The deployment information from the JSON file
        - The execution parameters passed to the DAG
        - The data passed from the previous task via xcom
        """
        base_task = BaseTask(context)
        base_task.summarize()

        api_key = base_task.find_xcom_target(ConfigurationConstants.COGSRCH_ADMIN_KEY)

        if not api_key or \
            ConfigurationConstants.COGSRCH_API_VERSION not in base_task.sideload_settings or \
            ConfigurationConstants.COGSRCH_INSTANCE not in base_task.sideload_settings or \
            ConfigurationConstants.COGSRCH_INDEXER not in base_task.sideload_settings:
            raise Exception("Data indexer  information is incomplete")
        
        api_version = base_task.sideload_settings[ConfigurationConstants.COGSRCH_API_VERSION]
        cog_search_inst = base_task.sideload_settings[ConfigurationConstants.COGSRCH_INSTANCE] 
        cog_search_indexer  = base_task.sideload_settings[ConfigurationConstants.COGSRCH_INDEXER]

        print("Key", api_key)
        print(api_version)
        print(cog_search_inst)
        print(cog_search_indexer)

        indexer_utils = CogSearchIndexerUtils(cog_search_inst, cog_search_indexer)
        print("Trigger and wait on indexer")

        processed_items = indexer_utils.start_and_monitor_indexer(api_key, api_version)
        print("Indexer finished with", processed_items, "items processed")
        return {ConfigurationConstants.COGSRCH_INDEXER_PROC_CNT : processed_items}