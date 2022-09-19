# Extract data from GG sheets
# -> Download file as excel file in working directory
from google.oauth2 import service_account
from getfilelistpy import getfilelist
import requests
from oauth2client.service_account import ServiceAccountCredentials
import os, sys
path = os.path.abspath('.')
sys.path.append(path)
from controller.processing_data import *
from controller.get_data import *

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'even-impulse-302623-7a2843af23d8.json'
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

topFolderId = '1xZe0mDUw-yfJhcKencXQeyqYoKxcjXsX' # Please set the folder of the top folder ID.
resource = {
    "service_account": credentials,
    "id": topFolderId,
    "fields": "files(name,id)",
}
res = getfilelist.GetFileList(resource)

credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, SCOPES)
access_token = credentials.create_delegated(credentials._service_account_email).get_access_token().access_token

file_name_list = []
for el in dict(res)['fileList'][0]['files']:
    url = "https://www.googleapis.com/drive/v3/files/" + el['id'] + "/export?mimeType=application%2Fvnd.openxmlformats-officedocument.spreadsheetml.sheet"
    res = requests.get(url, headers={"Authorization": "Bearer " + access_token})
    with open(f"temp/{el['name']}.xlsx", 'wb') as f:
        f.write(res.content)
    file_name_list.append(el['name'])

# Transfrom data in staging
# -> Export 2 DFs (1 df for finance, 1 df for order) with columns as per requirement
df = finalize_one_df_finance()


# Load data into Postgres DB
# -> 2 tables: 1 table for finance, 1 table for order