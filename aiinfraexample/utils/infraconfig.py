"""
Class used to parse the parameters from a context object from 
Airflow to teh DAG
"""

class Configuration:
    def __init__(self, context):
        if "params" in context:
            for param in context["params"]:
                setattr(self, param, context["params"][param])
