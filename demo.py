import pandas as pd 
import numpy as np
from datetime import datetime 
from datetime import date
import os, sys
from controller.processing_data import *
from controller.get_data import *

# today = date.today()
# today = str(today)
# print(today[2:4], today[5:7])
# print("Today's date:", today)


# df = pd.read_excel('temp/THU CHI T5-22.xlsx', sheet_name=0)
# df = df.iloc[2:,[1,2]]
# df['TỔNG THU'].replace('', np.nan, inplace=True)
# df.dropna(subset = ['TỔNG THU'], axis = 0, inplace=True)
# df['TỔNG THU'] = df['TỔNG THU'].apply(lambda x: x.strftime('%d/%m/%Y'))

# print(df)
# print(df['TỔNG THU'])
# print(df.dtypes)

# import json
# df = pd.read_excel('temp/THU CHI T5-22.xlsx', sheet_name='README')
# res = json.loads(df['sheets'].values[0])
# print(df['Month'].values[0])
# print(type(str(df['Month'].values[0])))
# print(type(res['DOANH-THU-NGAY']))

# today = str(date.today())
# current_term = 'THU CHI T' + today[5:7] + '-' + today[2:4]
# current_term = 'THU CHI T8-22'
# df = export_one_df_finance(current_term)
# ngay_number_from_source_max = int(df[df['DOANH-THU'] != 0]['NGAY-NUMBER'].max())

# df1 = df[df['DOANH-THU'] != 0]['NGAY'].max()
# print(df)
# print(df1)

# print(ngay_number_from_source_max)
# print(type(ngay_number_from_source_max))

# df1 = get_current_term_finance_db()
# ngay_number_from_db_max = df1[df1['doanh_thu'] != 0]['ngay_number'].max()
# print(ngay_number_from_db_max)
# print(type(ngay_number_from_db_max))

# ngay_number_list_to_add = []
# if ngay_number_from_source_max > ngay_number_from_db_max:
#     count = ngay_number_from_source_max - ngay_number_from_db_max
#     for i in range(count):
#         ngay_number_from_db_max = ngay_number_from_db_max + 1
#         ngay_number_list_to_add.append(ngay_number_from_db_max)
# print(df)
# print(ngay_number_list_to_add)

# new_dict = {}
# for i in ngay_number_list_to_add:
#     record = list(df[df['NGAY-NUMBER'] == str(i)][['DOANH-THU', 'CHI-PHI', 'NET-SP-FOOD', 'GRAB', 'BAEMIN', 'CK-SP-FOOD', 'SP-FOOD', 'CK-GRAB', 'CK-BAEMIN', 'TAI-QUAN', 'PCT-BAEMIN', 'PCT-GRAB', 'PCT-SP-FOOD', 'PCT-TAI-QUAN']].values[0])
#     record.append(i)
#     record = tuple(record)
#     new_dict[i] = record
# print(new_dict)

# part_stmt = "DOANH_THU = '%s', CHI_PHI = '%s', NET_SP_FOOD = '%s', GRAB = '%s', BAEMIN = '%s', CK_SP_FOOD = '%s', SP_FOOD = '%s', CK_GRAB = '%s', CK_BAEMIN = '%s', TAI_QUAN = '%s', PCT_BAEMIN = '%s', PCT_GRAB = '%s', PCT_SP_FOOD = '%s', PCT_TAI_QUAN = '%s' "

# for i in new_dict.values():
#     print(i)
#     final_stmt = "UPDATE finance SET " + part_stmt + "WHERE ngay_number = '%s'"
#     final_stmt = final_stmt % i
#     print(final_stmt)
#     engine.execute(final_stmt)

# print(today)
# print(type(today))

# date = datetime(2022, 9, 30)
# date = str(date)
# print(date)


#################################################################

# df = export_one_df_order('ORDER 04-22')

# Load data into Postgres DB
# -> 2 tables: 1 table for finance, 1 table for order
# root = "VALUES "
# loop = "('%s','%s','%s','%s','%s','%s','%s','%s','%s')"
# for i in range(df.shape[0]):
#     if i == 0:
#         root = root + loop
#     else:
#         root = root + ", " + loop

# ist_val = []
# for id, row in df.iterrows():
#     for i, val in enumerate(row):
#         try:
#             ist_val.append((val.strip()))
#         except:
#             ist_val.append(val)
        
# ist_val = tuple(ist_val)

# query_stmnt = "INSERT INTO orders (ngay_number, ky, ngay, so_hoa_don, ma_mon, ten_mon, sl_ban, don_gia, doanh_thu) " + root % ist_val
# engine.execute(query_stmnt)

if 2 < 1:
    print("2<1")
elif 3 > 2:
    print("3>2")
elif 6 > 5:
    print("6>5")