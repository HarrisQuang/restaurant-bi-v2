from google.oauth2 import service_account
from getfilelistpy import getfilelist
import gspread as gs 
import pandas as pd
from sqlalchemy import create_engine, text

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
print(res)
print('------------------------------')
print(dict(res)['fileList'])
print(dict(res)['fileList'][0]['files'])

# engine = create_engine("postgresql://postgres:12345678@localhost:5432/demo_db")
# name = 'finance_table'
# gc = gs.service_account('even-impulse-302623-7a2843af23d8.json')
# sh = gc.open_by_key('1mU0VhTKePNyaVZlfM6CgxdczAHMVXm0nKOkafnoM7g0')
# ws = sh.get_worksheet(0)
# df = pd.DataFrame(ws.get_values())
# print(df)
# df = df.iloc[3:34,[1,2]]
# df = df[(df[1] != '31/05/2022') & (df[1] != '30/05/2022')]
# df.columns = ['ngay', "so"]
# print(df)
# df.to_sql('%s' % (name),engine,index=False,if_exists='replace',chunksize=1000)

# engine = create_engine("postgresql://postgres:12345678@localhost:5432/demo_db")
# name = 'finance_table'
# source_df = pd.read_sql_table(name,engine)
# print(source_df)
# max = max(source_df.iloc[:,0])
# print(max)

# gc = gs.service_account('even-impulse-302623-7a2843af23d8.json')
# sh = gc.open_by_key('1mU0VhTKePNyaVZlfM6CgxdczAHMVXm0nKOkafnoM7g0')
# ws = sh.get_worksheet(0)
# df = pd.DataFrame(ws.get_values())
# df = df.iloc[3:34,[1,2]]
# df = df[df[1] > max]
# print(df)
# root = "VALUES "
# loop = "('%s','%s')"
# for i in range(df.shape[0]):
#     if i == 0:
#         root = root + loop
#     else:
#         root = root + ", " + loop
# print(root)

# ist_val = []
# for id, row in df.iterrows():
#     for i, val in enumerate(row):
#         ist_val.append((val.strip()))
        
# ist_val = tuple(ist_val)
# print(ist_val)

# query_stmnt = "INSERT INTO finance_table (Ngay, So) " + root % ist_val
# engine.execute("CREATE TABLE IF NOT EXISTS finance_table_1 (Ngay text, So text)")
# engine.execute(query_stmnt)