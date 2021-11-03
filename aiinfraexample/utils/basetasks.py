
import json
from pprint import pprint
from aiinfraexample.utils.infraconfig import Configuration

class BaseTask:
    XCOM_TARGET = "xcom_target"
    TASK_INSTANCE = "task_instance"
    TASK_PARAMS = "params"

    def __init__(self, context):
        self.context = context
        self.configuration = Configuration(context)
        self.xcom_target = None
        self.xcom_result = None

        if BaseTask.TASK_INSTANCE in context and BaseTask.XCOM_TARGET in context:
            self.xcom_target = context[BaseTask.XCOM_TARGET]
            self.xcom_result = context[BaseTask.TASK_INSTANCE].xcom_pull(task_ids=context[BaseTask.XCOM_TARGET])

            if self.xcom_result:
                try:
                    self.xcom_result = json.loads(self.xcom_result)
                except:
                    # Not a JSON object
                    pass


class Tasks:

    @staticmethod
    def process_storage(**context):
        return_value = None

        print("In process storage")
        base_task = BaseTask(context)
        print("Configuration", base_task.configuration.__dict__)

        if base_task.xcom_target and base_task.xcom_result:
            print("XCOM Target", base_task.xcom_target)

            print(json.dumps(base_task.xcom_result))
            return_value = json.dumps(base_task.xcom_result["storage_sas"])
        return return_value

    @staticmethod
    def store_results(**context):
        print("In store results")

        base_task = BaseTask(context)
        print("Configuration", base_task.configuration.__dict__)

        if base_task.xcom_target and base_task.xcom_result:
            print("XCOM Target", base_task.xcom_target)

            print(json.dumps(base_task.xcom_result))
