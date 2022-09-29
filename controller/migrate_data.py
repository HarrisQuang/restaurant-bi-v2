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
from sqlalchemy import create_engine, text
import shutil
from datetime import date

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

print(f'File list from data source: {file_name_list}')

term_in_db = get_finance_data_term_list()

print(f'Term list from DB: {term_in_db}')

file_name_migrate = []
for i in file_name_list:
    count = 0
    for j in term_in_db:
        if i == j:
            count += 1
    if count == 0:
        file_name_migrate.append(i)

print(f'{file_name_migrate}')

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
current_term = 'THU CHI T9-22'
df = export_one_df_finance(current_term)
if df != None:
    ngay_number_from_source_max = int(df[df['DOANH-THU'] != 0]['NGAY-NUMBER'].max())
    print(ngay_number_from_source_max)
    print(type(ngay_number_from_source_max))

    df1 = get_current_term_finance_db()
    ngay_number_from_db_max = df1[df1['doanh_thu'] != 0]['ngay_number'].max()
    print(ngay_number_from_db_max)
    print(type(ngay_number_from_db_max))

    ngay_number_list_to_add = []
    if ngay_number_from_source_max > ngay_number_from_db_max:
        count = ngay_number_from_source_max - ngay_number_from_db_max
        for i in range(count):
            ngay_number_from_db_max = ngay_number_from_db_max + 1
            ngay_number_list_to_add.append(ngay_number_from_db_max)
        print(df)
        print(ngay_number_list_to_add)

        new_dict = {}
        for i in ngay_number_list_to_add:
            record = list(df[df['NGAY-NUMBER'] == str(i)][['DOANH-THU', 'CHI-PHI', 'NET-SP-FOOD', 'GRAB', 'BAEMIN', 'CK-SP-FOOD', 'SP-FOOD', 'CK-GRAB', 'CK-BAEMIN', 'TAI-QUAN', 'PCT-BAEMIN', 'PCT-GRAB', 'PCT-SP-FOOD', 'PCT-TAI-QUAN']].values[0])
            record.append(i)
            record = tuple(record)
            new_dict[i] = record
        print(new_dict)

        part_stmt = "DOANH_THU = '%s', CHI_PHI = '%s', NET_SP_FOOD = '%s', GRAB = '%s', BAEMIN = '%s', CK_SP_FOOD = '%s', SP_FOOD = '%s', CK_GRAB = '%s', CK_BAEMIN = '%s', TAI_QUAN = '%s', PCT_BAEMIN = '%s', PCT_GRAB = '%s', PCT_SP_FOOD = '%s', PCT_TAI_QUAN = '%s' "

        for i in new_dict.values():
            print(i)
            final_stmt = "UPDATE finance SET " + part_stmt + "WHERE ngay_number = '%s'"
            final_stmt = final_stmt % i
            print(final_stmt)
            engine.execute(final_stmt)