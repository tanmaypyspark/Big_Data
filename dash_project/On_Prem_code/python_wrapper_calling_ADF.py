############################################################################################
#  
#   C:\Users\tanma\AppData\Local\Programs\Python\Python310\python.exe C:\Users\tanma\OneDrive\Desktop\GIT_PUSH\Big_Data\Dash_project\On_Prem_code\python_wrapper_calling_ADF.py
#
#
#
############################################################################################
#from azure.identity import DefaultAzureCredential

from azure.mgmt.datafactory import DataFactoryManagementClient

## Authenticate using default Azure credentials
#
#credential = DefaultAzureCredential()
#
#adf_client = DataFactoryManagementClient(credential, "<subscription-id>")
#
## Resource Group and Data Factory names
#
#resource_group_name = "<resource-group-name>"
#
#data_factory_name = "<data-factory-name>"
#
#trigger_name = "<trigger-name>"
#
## Start the trigger
#
#adf_client.triggers.begin_start(resource_group_name, data_factory_name, trigger_name)