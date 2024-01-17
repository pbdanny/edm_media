# Databricks notebook source
import requests

url = 'https://prod-57.southeastasia.logic.azure.com:443/workflows/797a686f6661423f975f39023c5a3eba/triggers/request/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Frequest%2Frun&sv=1.0&sig=YEQbfXqVDmraublUvgGGY8esCSsINC3w_mxhPc3z7Z4'

headers = {
    'Content-Type': 'application/json',
}

data = {
    "pipeline_name": "PL_EDM_MANUAL_JOB_CLUSTER",
    "notebookPath": ["/Repos/thanakrit.boonquarmdee@lotuss.com/edm_media_dev/notebook/dev_code_o2o"],
    "job_type": "standard",
    "clusterSize": "s",
    "period_id": "",
    "week_id": "",
    "date_id": "",
    "gchat_message": "Start Job : https://adf.azure.com/en/monitoring/pipelineruns?factory=%2Fsubscriptions%2F1d9b0272-2204-483b-a0d5-f8eb507015db%2FresourceGroups%2Fpvtdmrsgazc01%2Fproviders%2FMicrosoft.DataFactory%2Ffactories%2Fpvtdmadfazc01",
    "gchat_token": "https://chat.googleapis.com/v1/spaces/AAAAW6qavWw/messages?key=AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI&token=vZxcgUjqj7QJZpu-inizlLqZzj9PAK1LgYMd4Jk660I",
    "CallBackUri": "URL"
}

response = requests.post(url, headers=headers, json=data)

# Check the response
if response.status_code == 200:
    print("Request was successful.")
    print(response.text)
else:
    print("Request failed with status code:", response.status_code)
    print(response.text)

# COMMAND ----------


