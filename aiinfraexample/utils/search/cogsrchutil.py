import time
import requests
from datetime import datetime
import json

class CogSearchStorageDetails:
    def __init__(self):
        self.account_name = None
        self.container_name = None
        self.folder_path = None


class CogSearchDataSourceUtils:

    """ Get datasource information
    https://docs.microsoft.com/en-us/rest/api/searchservice/get-data-source
        GET https://[service name].search.windows.net/datasources/[data source name]?api-version=[api-version]&includeConnectionString=[includeConnectionString]
            Content-Type: application/json  
            api-key: [admin key]  
    """
    DATA_SOURCE_INFO_URI = "https://{}.search.windows.net/datasources/{}?api-version={}&includeConnectionString=true"

    def __init__(self,cog_srch_instance: str, datasource_name:str):
        self.cog_srch_instance = cog_srch_instance
        self.datasource_name = datasource_name

    def get_datasource_details(self, api_key:str, api_version:str) -> CogSearchStorageDetails:
        uri = CogSearchDataSourceUtils.DATA_SOURCE_INFO_URI.format(
            self.cog_srch_instance,
            self.datasource_name,
            api_version
        )

        headers = {
            "Content-Type" : "application/json",
            "api-key" : api_key
        }

        response = requests.get(uri, headers=headers)
        return_result = CogSearchStorageDetails()
        if response.status_code == 200:
            json_response = response.json()

            if "credentials" not in json_response:
                raise Exception("credentials missing from datasource response")
            
            if "connectionString" not in json_response["credentials"]:
                raise Exception("connectionString missing from credentials")
            
            if "container" not in json_response:
                raise Exception("container missing from datasource response")

            # Parse out account name
            connection = json_response["credentials"]["connectionString"]
            account_idx = connection.index("AccountName=") + len("AccountName=")
            connection = connection[account_idx:]
            end_idx = connection.index(';')

            return_result.account_name = connection[:end_idx]

            # Get the container informatin
            container = json_response["container"]
            if "name" in container:
                return_result.container_name = container["name"]
            if "query" in container:
                return_result.folder_path = container["query"]

        return return_result

class CogSearchRestUtils:

    """
    https://docs.microsoft.com/en-us/rest/api/searchservice/search-documents
        POST https://[service name].search.windows.net/indexes/[index name]/docs/search?api-version=[api-version]  
            Content-Type: application/json  
            api-key: [admin or query key]    
    """
    SEARCH_DOCS_URI = "https://{}.search.windows.net/indexes/{}/docs/search?api-version={}"

    def __init__(self, cog_srch_instance: str, index_name:str):
        self.cog_srch_instance = cog_srch_instance
        self.cog_srch_index = index_name

    def find_file(self, file_name:str, api_key:str, api_version:str ):

        search_uri = CogSearchRestUtils.SEARCH_DOCS_URI.format(
            self.cog_srch_instance,
            self.cog_srch_index,
            api_version
        )

        headers = {
            "Content-Type" : "application/json",
            "api-key" : api_key
        }

        data = {  
            "search": file_name,  
            "searchMode" : "all",
            "searchFields": "metadata_storage_name",  
            "select": "*"  
        } 


        return_data = None
        response = requests.post(search_uri, headers=headers, json=data)
        if response.status_code == 200:
            result_data = response.json()

            if isinstance(result_data, dict):
                print("Response was a dictionary")
                return_data = result_data
            elif isinstance(result_data, list):
                return_data = result_data[0]
        
        return return_data

class CogSearchIndexerUtils:
    """ Running an indexer
        https://docs.microsoft.com/en-us/rest/api/searchservice/run-indexer

        POST https://[service name].search.windows.net/indexers/[indexer name]/run?api-version=[api-version]  
            Content-Type: application/json  
            api-key: [admin key]
    """
    RUN_INDEXER_URI = "https://{}.search.windows.net/indexers/{}/run?api-version={}"
    """ Getting indexer status
        https://docs.microsoft.com/en-us/rest/api/searchservice/get-indexer-status
        
        GET https://[service name].search.windows.net/indexers/[indexer name]/status?api-version=[api-version]&failIfCannotDecrypt=[true|false]
            Content-Type: application/json  
            api-key: [admin key]    
    """
    INDEXER_STATUS_URI = "https://{}.search.windows.net/indexers/{}/status?api-version={}"

    def __init__(self, cog_srch_instance: str, indexer_name:str):
        self.cog_srch_instance = cog_srch_instance
        self.cog_srch_indexer = indexer_name

    def start_and_monitor_indexer(self, api_key:str, api_version:str,  timeout_mins:int = 10) -> int:

        # <0 -> error like a timeout, >=0 is the number of processed items
        return_status = -1

        starting_status = self.get_indexer_status(api_key, api_version)

        print("Starting indexer", self.cog_srch_indexer)
        if not self.start_indexer(api_key, api_version):
            raise Exception("Indexer failed to start")

        total_wait_seconds = timeout_mins * 60
        start_time = datetime.utcnow()

        while True:
            current_status = self.get_indexer_status(api_key, api_version)
            time.sleep(10)
            current_run_time = (datetime.utcnow() - start_time).total_seconds()

            if starting_status["lastResult"]["startTime"] != current_status["lastResult"]["startTime"]:
                return_status = current_status["lastResult"]["itemsProcessed"]
                print(self.cog_srch_indexer, "run is finished in", current_run_time, "seconds")
                break

            if int(current_run_time) > total_wait_seconds:
                print("You have run out of time")
                break
            
            # Else lets sleep for 10 seconds and see what happens next
            print("No result yet.... in ", current_run_time, "seconds")

        return return_status

    def start_indexer(self, api_key:str, api_version:str) -> bool:
        uri = CogSearchIndexerUtils.RUN_INDEXER_URI.format(
            self.cog_srch_instance,
            self.cog_srch_indexer,
            api_version
        )

        headers = {
            "Content-Type" : "application/json",
            "api-key" : api_key
        }

        response = requests.post(uri, headers=headers)
        return response.status_code == 202

    def get_indexer_status(self, api_key:str, api_version:str) -> dict:
        uri = CogSearchIndexerUtils.INDEXER_STATUS_URI.format(
            self.cog_srch_instance,
            self.cog_srch_indexer,
            api_version
        )

        headers = {
            "Content-Type" : "application/json",
            "api-key" : api_key
        }

        response = requests.get(uri, headers=headers)
        return_result = {}
        if response.status_code == 200:
            json_response = response.json()
            if "status" in json_response:
                return_result["status"] = json_response["status"]
            if "lastResult" in json_response:
                return_result["lastResult"] = json_response["lastResult"]

        return return_result


