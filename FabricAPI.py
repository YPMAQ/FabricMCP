from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential
from io import BytesIO
import pandas as pd
from azure.identity import DeviceCodeCredential

def get_token_via_browser():
    credential = DefaultAzureCredential()
    token = credential.get_token("https://storage.azure.com/.default")
    return token.token

def upload_file_to_lakehouse(account_name,WORKSPACE_NAME,local_file_path, lakehouse_file_path):
    account_url = f"https://{account_name}.dfs.fabric.microsoft.com"
    token_credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url, credential=token_credential)
    file_system_client = service_client.get_file_system_client(WORKSPACE_NAME)
    file_client = file_system_client.get_file_client(lakehouse_file_path)
    
    # Read local file and upload
    with open(local_file_path, "rb") as data:
        file_contents = data.read()
        file_client.create_file()
        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))
    print(f"Uploaded {local_file_path} to {lakehouse_file_path}")

def download_file_from_lakehouse(ACCOUNT_NAME,WORKSPACE_NAME, lakehouse_file_path):
    account_url = f"https://{ACCOUNT_NAME}.dfs.fabric.microsoft.com"
    token_credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url, credential=token_credential)
    file_system_client = service_client.get_file_system_client(WORKSPACE_NAME)
    
    # Get the file client for the file you want to download
    file_client = file_system_client.get_file_client(lakehouse_file_path)
    
    # Download the file
    download = file_client.download_file()
    downloaded_bytes = download.readall()
    df = pd.read_csv(BytesIO(downloaded_bytes))
    return df

def upload_logfile_to_lakehouse(account_name, WORKSPACE_NAME, lakehouse_file_path, file_bytes=None):
    account_url = f"https://{account_name}.dfs.fabric.microsoft.com"
    token_credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url, credential=token_credential)
    file_system_client = service_client.get_file_system_client(WORKSPACE_NAME)
    file_client = file_system_client.get_file_client(lakehouse_file_path)
    file_contents = file_bytes    
    file_client.create_file()
    file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
    file_client.flush_data(len(file_contents))
    print(f"Uploaded Log file to {lakehouse_file_path}")

def list_items(ACCOUNT_NAME,WORKSPACE_NAME,DATA_PATH):
    #Create a service client using the default Azure credential

    account_url = f"https://{ACCOUNT_NAME}.dfs.fabric.microsoft.com"
    # token_credential = DefaultAzureCredential()
    token_credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url, credential=token_credential)

    #Create a file system client for the workspace
    file_system_client = service_client.get_file_system_client(WORKSPACE_NAME)
    
    #List a directory within the filesystem
    paths = file_system_client.get_paths(path=DATA_PATH)

    return paths
