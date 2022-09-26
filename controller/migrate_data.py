# Extract data from GG sheets
# -> Download file as excel file in working directory
from google.oauth2 import service_account
from getfilelistpy import getfilelist
import requests
from oauth2client.service_account import ServiceAccountCredentials
import os, sys
from controller.processing_data import *
from controller.get_data import *
from sqlalchemy import create_engine, text
import shutil
from datetime import date

path = os.path.abspath('.')
sys.path.append(path)

shutil.rmtree('temp')
os.mkdir('temp', 0o755)

engine = create_engine("postgresql://postgres:12345678@localhost:5432/demo_db")

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

term_in_db = get_finance_data_term_list()

file_name_migrate = []
for i in file_name_list:
    for j in term_in_db:
        if i != j:
            file_name_migrate.append(i)
            
# Transfrom data in staging
# -> Export 2 DFs (1 df for finance, 1 df for order) with columns as per requirement
for name in file_name_migrate:
    df = export_one_df_finance(name)

    # Load data into Postgres DB
    # -> 2 tables: 1 table for finance, 1 table for order
    root = "VALUES "
    loop = "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    for i in range(df.shape[0]):
        if i == 0:
            root = root + loop
        else:
            root = root + ", " + loop
    print(root)

    ist_val = []
    for id, row in df.iterrows():
        for i, val in enumerate(row):
            try:
                ist_val.append((val.strip()))
            except:
                ist_val.append(val)
            
    ist_val = tuple(ist_val)
    print(ist_val)

    query_stmnt = "INSERT INTO finance (NGAY_NUMBER, KY, NGAY, DOANH_THU, CHI_PHI, NET_SP_FOOD, GRAB, BAEMIN, CK_SP_FOOD, SP_FOOD, CK_GRAB, CK_BAEMIN, TAI_QUAN, PCT_BAEMIN, PCT_GRAB, PCT_SP_FOOD, PCT_TAI_QUAN) " + root % ist_val
    engine.execute(query_stmnt)
    
today = str(date.today())
current_term = 'THU CHI T' + today[6:7] + '-' + today[2:4]
df = export_one_df_finance(current_term)

