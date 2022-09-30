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
from datetime import datetime, date
import time

now = datetime.now().strftime("%d_%m_%Y %H_%M_%S")

start = time.time()
try:
    shutil.rmtree('temp/order/')
except:
    pass
os.mkdir('temp/order/', 0o755)

list_to_log = []
list_to_log.append(f'Datetime: {now}')
print('Start getting files from data source')
engine = create_engine("postgresql://postgres:12345678@localhost:5432/demo_db")

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = 'even-impulse-302623-7a2843af23d8.json'
credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

topFolderId = '1_r-0dW3gkcCHfncLJN1jrG6qjEnYf1Z5' # Please set the folder of the top folder ID.
resource = {
    "service_account": credentials,
    "id": topFolderId,
    "fields": "files(name,id)",
}
res = getfilelist.GetFileList(resource)

credentials = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, SCOPES)
access_token = credentials.create_delegated(credentials._service_account_email).get_access_token().access_token

file_name_list = []
file_list_from_source = dict(res)['fileList'][0]['files']
for count, el in enumerate(file_list_from_source):
    print(f"[{count+1}/{len(file_list_from_source)}] Processing the file {el['name']}")
    url = "https://www.googleapis.com/drive/v3/files/" + el['id'] + "/export?mimeType=application%2Fvnd.openxmlformats-officedocument.spreadsheetml.sheet"
    res = requests.get(url, headers={"Authorization": "Bearer " + access_token})
    with open(f"temp/order/{el['name']}.xlsx", 'wb') as f:
        f.write(res.content)
    file_name_list.append(el['name'])

if len(file_name_list) != 0:
    line_2 = f'--> File list from data source: {file_name_list}'
    print(line_2)
    list_to_log.append(line_2)
else:
    line_2 = "There's no data in data source"
    print(line_2)
    list_to_log.append(line_2)

term_in_db = get_order_data_term_list()

if len(term_in_db) != 0:
    line_3 = f'Existing term list from DB: {term_in_db}'
    print(line_3)
    list_to_log.append(line_3)
else:
    line_3 = "There's no data in DB"
    print(line_3)
    list_to_log.append(line_3)
    
file_name_migrate = []
for i in file_name_list:
    count = 0
    for j in term_in_db:
        if i == j:
            count += 1
    if count == 0:
        file_name_migrate.append(i)

if len(file_name_migrate) != 0:
    line_4 = f'--> Files need to be migrated: {file_name_migrate}'
    print(line_4)
    list_to_log.append(line_4)
else:
    line_4 = 'No new files need to be migrated'
    print(line_4)
    list_to_log.append(line_4)

# Transfrom data in staging
# -> Export 2 DFs (1 df for finance, 1 df for order) with columns as per requirement
for count, name in enumerate(file_name_migrate):
    print(f'[{count+1}/{len(file_name_migrate)}] Processing file: {name}')
    df = export_one_df_order(name)
    print(df.head(10))
    # Load data into Postgres DB
    # -> 2 tables: 1 table for finance, 1 table for order
#     root = "VALUES "
#     loop = "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
#     for i in range(df.shape[0]):
#         if i == 0:
#             root = root + loop
#         else:
#             root = root + ", " + loop

#     ist_val = []
#     for id, row in df.iterrows():
#         for i, val in enumerate(row):
#             try:
#                 ist_val.append((val.strip()))
#             except:
#                 ist_val.append(val)
            
#     ist_val = tuple(ist_val)
#     print(ist_val)

#     query_stmnt = "INSERT INTO finance (NGAY_NUMBER, KY, NGAY, DOANH_THU, CHI_PHI, NET_SP_FOOD, GRAB, BAEMIN, CK_SP_FOOD, SP_FOOD, CK_GRAB, CK_BAEMIN, TAI_QUAN, PCT_BAEMIN, PCT_GRAB, PCT_SP_FOOD, PCT_TAI_QUAN) " + root % ist_val
#     engine.execute(query_stmnt)

# today = str(date.today())
# current_term = 'THU CHI T' + today[6:7] + '-' + today[2:4]
# current_term = 'THU CHI T9-22'
# df = export_one_df_finance(current_term)
# if df != None:
#     ngay_number_from_source_max = int(df[df['DOANH-THU'] != 0]['NGAY-NUMBER'].max())

#     df1 = get_current_term_finance_db()
#     ngay_number_from_db_max = df1[df1['doanh_thu'] != 0]['ngay_number'].max()

#     ngay_number_list_to_add = []
#     if ngay_number_from_source_max > ngay_number_from_db_max:
#         count = ngay_number_from_source_max - ngay_number_from_db_max
#         for i in range(count):
#             ngay_number_from_db_max = ngay_number_from_db_max + 1
#             ngay_number_list_to_add.append(ngay_number_from_db_max)
        
#         line_5 = f'List of days need to be appended: {ngay_number_list_to_add}'
#         print(line_5)
#         list_to_log.append(line_5)
        
#         part_stmt = "DOANH_THU = '%s', CHI_PHI = '%s', NET_SP_FOOD = '%s', GRAB = '%s', BAEMIN = '%s', CK_SP_FOOD = '%s', SP_FOOD = '%s', CK_GRAB = '%s', CK_BAEMIN = '%s', TAI_QUAN = '%s', PCT_BAEMIN = '%s', PCT_GRAB = '%s', PCT_SP_FOOD = '%s', PCT_TAI_QUAN = '%s' "
#         new_dict = {}
#         for count, i in enumerate(ngay_number_list_to_add):
#             print(f'[{count + 1}/{len(ngay_number_list_to_add)}] The ngay_number {i} are adding')
#             record = list(df[df['NGAY-NUMBER'] == str(i)][['DOANH-THU', 'CHI-PHI', 'NET-SP-FOOD', 'GRAB', 'BAEMIN', 'CK-SP-FOOD', 'SP-FOOD', 'CK-GRAB', 'CK-BAEMIN', 'TAI-QUAN', 'PCT-BAEMIN', 'PCT-GRAB', 'PCT-SP-FOOD', 'PCT-TAI-QUAN']].values[0])
#             record.append(i)
#             record = tuple(record)
#             final_stmt = "UPDATE finance SET " + part_stmt + "WHERE ngay_number = '%s'"
#             final_stmt = final_stmt % record
#             engine.execute(final_stmt)
#     else:
#         line_5 = "There's no any new records needed to be fetched"
#         print(line_5)
#         list_to_log.append(line_5)
# else:
#     line_5 = f"The current term {current_term} is not available"
#     print(line_5)
#     list_to_log.append(line_5)
# end = time.time()
# line_6 = f'Whole process takes {end - start}'
# print(line_6)
# list_to_log.append(line_6)

# with open(f'log/order_{now}.txt', 'w') as f:
#     for line in list_to_log:
#         f.write(line)
#         f.write('\n')