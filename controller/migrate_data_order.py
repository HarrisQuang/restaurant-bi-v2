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

engine.execute("CREATE TABLE IF NOT EXISTS orders (ngay_number integer, ky text, ngay text, so_hoa_don text, ma_mon text, ten_mon text, sl_ban real, don_gia real, doanh_thu real)")

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

    # Load data into Postgres DB
    # -> 2 tables: 1 table for finance, 1 table for order
    root = "VALUES "
    loop = "('%s','%s','%s','%s','%s','%s','%s','%s','%s')"
    for i in range(df.shape[0]):
        if i == 0:
            root = root + loop
        else:
            root = root + ", " + loop

    ist_val = []
    for id, row in df.iterrows():
        for i, val in enumerate(row):
            try:
                ist_val.append((val.strip()))
            except:
                ist_val.append(val)
            
    ist_val = tuple(ist_val)

    query_stmnt = "INSERT INTO orders (ngay_number, ky, ngay, so_hoa_don, ma_mon, ten_mon, sl_ban, don_gia, doanh_thu) " + root % ist_val
    engine.execute(query_stmnt)

today = str(date.today())
current_term = 'ORDER ' + today[5:7] + '-' + today[2:4]
df = export_one_df_order(current_term)
if df is not None:
    so_hd_list_source = df['Số hóa đơn'].unique()
    
    df1 = get_current_term_order_db()
    so_hd_list_db = df1['so_hoa_don'].unique()

    so_hd_list_to_add = []
        
    if len(so_hd_list_source) > len(so_hd_list_db):
        for hd_source in so_hd_list_source:
            count = 0
            for hd_db in so_hd_list_db:
                if hd_source == hd_db:
                    count += 1
                    break
            if count == 0:
                so_hd_list_to_add.append(hd_source)
        
        line_5 = f'List of orders need to be appended: {so_hd_list_to_add}'
        print(line_5)
        list_to_log.append(line_5)
        
        part_df = []
        for so_hd in so_hd_list_to_add:
            new_df = df[df['Số hóa đơn'] == so_hd]
            part_df.append(new_df)
        final_df = pd.concat(part_df, axis=0)           
            
        root = "VALUES "
        loop = "('%s','%s','%s','%s','%s','%s','%s','%s','%s')"
        for i in range(final_df.shape[0]):
            if i == 0:
                root = root + loop
            else:
                root = root + ", " + loop

        ist_val = []
        for id, row in final_df.iterrows():
            for i, val in enumerate(row):
                try:
                    ist_val.append((val.strip()))
                except:
                    ist_val.append(val)
                
        ist_val = tuple(ist_val)

        query_stmnt = "INSERT INTO orders (ngay_number, ky, ngay, so_hoa_don, ma_mon, ten_mon, sl_ban, don_gia, doanh_thu) " + root % ist_val
        engine.execute(query_stmnt)
    else:
        line_5 = "There's no any new records needed to be fetched"
        print(line_5)
        list_to_log.append(line_5)
else:
    line_5 = f"The current term {current_term} is not available"
    print(line_5)
    list_to_log.append(line_5)
end = time.time()
line_6 = f'Whole process takes {end - start}'
print(line_6)
list_to_log.append(line_6)

with open(f'log/order_{now}.txt', 'w') as f:
    for line in list_to_log:
        f.write(line)
        f.write('\n')