import pandas as pd
from sqlalchemy import create_engine, text
import os, sys
path = os.path.abspath('.')
sys.path.append(path)
from controller.processing_data import *
from controller.get_data import *

engine = create_engine("postgresql://postgres:12345678@localhost:5432/demo_db")

df = export_one_df_finance('THU CHI T8-22')

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
        # ist_val.append((val.strip()))
        ist_val.append(val)
        
ist_val = tuple(ist_val)
print(ist_val)

query_stmnt = "INSERT INTO finance (NGAY_NUMBER, KY, NGAY, DOANH_THU, CHI_PHI, NET_SP_FOOD, GRAB, BAEMIN, CK_SP_FOOD, SP_FOOD, CK_GRAB, CK_BAEMIN, TAI_QUAN, PCT_BAEMIN, PCT_GRAB, PCT_SP_FOOD, PCT_TAI_QUAN) " + root % ist_val
engine.execute(query_stmnt)

# name = 'finance'
# source_df = pd.read_sql_table(name,engine)
# print(source_df.head())

####update data
ngay_number = 123
part_stmt = "DOANH_THU = '%s', CHI_PHI = '%s', NET_SP_FOOD = '%s', GRAB = '%s', BAEMIN = '%s', CK_SP_FOOD = '%s', SP_FOOD = '%s', CK_GRAB = '%s', CK_BAEMIN = '%s', TAI_QUAN = '%s', PCT_BAEMIN = '%s', PCT_GRAB = '%s', PCT_SP_FOOD = '%s', PCT_TAI_QUAN = '%s' "
final_stmt = "UPDATE finance SET " + part_stmt + "WHERE ngay_number = '%s'" 
engine.execute(final_stmt)