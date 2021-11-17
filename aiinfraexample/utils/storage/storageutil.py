import os
from azure.storage.blob import (
    BlobServiceClient,
    PublicAccess 
)

class AzureStorageUtil:
    BLOB_STORE_URI_TEMPLATE = "https://{}.blob.core.windows.net/"

    def __init__(self, storage_account_name, credentials):
        self.credentials = credentials
        self.account_name = storage_account_name
        self.store_uri = AzureStorageUtil.BLOB_STORE_URI_TEMPLATE.format(
            self.account_name
        )

        self.blob_svc_client = BlobServiceClient(
                account_url=self.store_uri,
                credential=self.credentials
                )    

    def download_blob(self, container:str, blob_path:str, file_path:str):
        return_value = False

        if os.path.exists(file_path):
            os.remove(file_path)

        blob_client = self.blob_svc_client.get_blob_client(
            container,
            blob_path
        )

        if blob_client.exists():
            return_value = True
            with open(file_path , "wb") as blob_instance:
                download_stream = blob_client.download_blob()
                blob_instance.write(download_stream.readall())
                
        return return_value

    def upload_process_file(self, container:str, blob_path:str, file_path:str ):
        if not os.path.exists(file_path):
            raise Exception("File doesn't exist")

        blob_client = self.blob_svc_client.get_blob_client(
            container,
            blob_path
        )

        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, blob_type="BlockBlob")

        print("uploaded")

    def delete_process_file(self, container: str, file_name: str):
        result = self._process_file_exists(container, file_name)
        if result[0]:
            print("Found and deleting")
            result[1].delete_blob()
            

    def _process_file_exists(self, container: str, file_name: str) -> tuple:

        if not self._container_exists(container):
            self._create_container(container)
            return (False, None)

        blob_client = self.blob_svc_client.get_blob_client(
            container,
            file_name
        )

        return (blob_client.exists(), blob_client)

    def _container_exists(self, container_name) -> bool:
        client =  self.blob_svc_client.get_container_client(container_name)    
        return client.exists()

    def _create_container(self, container_name):
        self.blob_svc_client.create_container(container_name)

        # Set the permission so the blobs are public.
        self.blob_svc_client.set_container_acl(
            container_name, 
            public_access=PublicAccess.OFF
        )      

"""
AZ_CREDS = DefaultAzureCredential()
STG_ACCOUNT_NAME = "dagstorage"
STG_ACCOUNT_CONTAINER = "dummy"
LOCAL_FILE = "exampleconf.json"

FULL_LOCAL = os.path.join(os.path.split(__file__)[0], LOCAL_FILE)

azstg = AzureStorageUtil(STG_ACCOUNT_NAME, AZ_CREDS)
azstg.delete_process_file(STG_ACCOUNT_CONTAINER, LOCAL_FILE)
azstg.upload_process_file(STG_ACCOUNT_CONTAINER, FULL_LOCAL)
"""
