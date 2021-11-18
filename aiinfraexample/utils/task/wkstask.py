from aiinfraexample.utils import ConfigurationConstants
from aiinfraexample.utils.task.basetasks import BaseTask

class WellKnownSchemaTask:

    @staticmethod
    def massage_processed_record(**context):
        print("In well known schema task details")

        """
        Create a BaseTask object that will parse the context and provide simple access to
        - The deployment information from the JSON file
        - The execution parameters passed to the DAG
        - The data passed from the previous task via xcom
        """
        base_task = BaseTask(context)
        base_task.summarize()

        processed_file_content = base_task.find_xcom_target(ConfigurationConstants.COGSRCH_RESULT_CONTENT)

        print("Have the processed file:", processed_file_content is not None)

