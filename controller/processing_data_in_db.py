# Extract data from GG sheets
# -> Download file as excel file in working directory
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

# list_to_log = []
# list_to_log.append(f'Datetime: {now}')
print('Start processing data in DB')
# engine = create_engine("postgresql://postgres:12345678@localhost:5432/demo_db")

# engine.execute("CREATE TABLE IF NOT EXISTS resolve_overlap_dish_remove_extra_fee (ngay text, cycle text, ma_mon text, ten_mon text, sl_ban real, doanh_thu real, don_gia real, ngay_number integer)")

order_data = get_order_data_from_db()
tuning_order_data = resolve_overlap_dish_remove_extra_fee(order_data)
# tuning_order_data = tuning_order_data.iloc[:2]

# Transfrom data in staging
# -> Export 2 DFs (1 df for finance, 1 df for order) with columns as per requirement

# Load data into Postgres DB
# -> 2 tables: 1 table for finance, 1 table for order
# root = "VALUES "
# loop = "('%s','%s','%s','%s','%s','%s','%s','%s')"
# for i in range(tuning_order_data.shape[0]):
#     if i == 0:
#         root = root + loop
#     else:
#         root = root + ", " + loop

# ist_val = []
# for id, row in tuning_order_data.iterrows():
#     for i, val in enumerate(row):
#         try:
#             ist_val.append((val.strip()))
#         except:
#             ist_val.append(val)
        
# ist_val = tuple(ist_val)

# query_stmnt = "INSERT INTO resolve_overlap_dish_remove_extra_fee (ngay, cycle, ten_mon, ma_mon, sl_ban, don_gia, doanh_thu, ngay_number) " + root % ist_val
# engine.execute(query_stmnt)

# resolve_overlap_remove_extra_data = get_resolve_overlap_remove_extra_data_from_db()
# statistic_dish = statistic_dish_by_cycle(resolve_overlap_remove_extra_data)
# print(statistic_dish.head())
# print(statistic_dish.columns)

# engine.execute("CREATE TABLE IF NOT EXISTS statistic_dish_by_cycle (cycle text, ten_mon text, tong_sl_ban real, max_sl_ban real, min_sl_ban real, avg_sl_ban real, median_sl_ban real, mode_sl_ban real, cycle_number integer, flag integer, pct_tong_sl_ban text, pct_max_sl_ban text, pct_min_sl_ban text, pct_avg_sl_ban text, pct_median_sl_ban text, pct_mode_sl_ban text)")

# root = "VALUES "
# loop = "('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"
# for i in range(statistic_dish.shape[0]):
#     if i == 0:
#         root = root + loop
#     else:
#         root = root + ", " + loop

# ist_val = []
# for id, row in statistic_dish.iterrows():
#     for i, val in enumerate(row):
#         try:
#             ist_val.append((val.strip()))
#         except:
#             ist_val.append(val)
        
# ist_val = tuple(ist_val)

# query_stmnt = "INSERT INTO statistic_dish_by_cycle (cycle, ten_mon, tong_sl_ban, max_sl_ban, min_sl_ban, avg_sl_ban, median_sl_ban, mode_sl_ban, cycle_number, flag, pct_tong_sl_ban, pct_max_sl_ban, pct_min_sl_ban, pct_avg_sl_ban, pct_median_sl_ban, pct_mode_sl_ban) " + root % ist_val
# engine.execute(query_stmnt)

######################################################################

total_order_vegan_day = generate_total_order_vegan_day()

# engine.execute("CREATE TABLE IF NOT EXISTS total_order_vegan_day (ngay_number integer, ngay text, total_order integer, ngay_duong_number integer, ngay_duong text, ngay_am text, ngay_filter text)")

# root = "VALUES "
# loop = "('%s','%s','%s', '%s','%s','%s','%s')"
# for i in range(total_order_vegan_day.shape[0]):
#     if i == 0:
#         root = root + loop
#     else:
#         root = root + ", " + loop

# ist_val = []
# for id, row in total_order_vegan_day.iterrows():
#     for i, val in enumerate(row):
#         try:
#             ist_val.append((val.strip()))
#         except:
#             ist_val.append(val)
        
# ist_val = tuple(ist_val)

# query_stmnt = "INSERT INTO total_order_vegan_day (ngay_number, ngay, total_order, ngay_duong_number, ngay_duong, ngay_am, ngay_filter) " + root % ist_val
# engine.execute(query_stmnt)

################################################################

total_sale_grouping_cycle_dish = generate_total_sale_grouping_cycle_and_dish()

engine.execute("CREATE TABLE IF NOT EXISTS total_sale_grouping_cycle_dish (cycle text, ten_mon text, don_gia float, total_sl_ban float, dthu float)")

root = "VALUES "
loop = "('%s','%s','%s','%s','%s')"
for i in range(total_sale_grouping_cycle_dish.shape[0]):
    if i == 0:
        root = root + loop
    else:
        root = root + ", " + loop

ist_val = []
for id, row in total_sale_grouping_cycle_dish.iterrows():
    for i, val in enumerate(row):
        try:
            ist_val.append((val.strip()))
        except:
            ist_val.append(val)
        
ist_val = tuple(ist_val)

query_stmnt = "INSERT INTO total_sale_grouping_cycle_dish (cycle, ten_mon, don_gia, total_sl_ban, dthu) " + root % ist_val
engine.execute(query_stmnt)

########################################################################




end = time.time()
line_6 = f'Whole process takes {end - start}'
print(line_6)
# list_to_log.append(line_6)

# with open(f'log/order_{now}.txt', 'w') as f:
#     for line in list_to_log:
#         f.write(line)
#         f.write('\n')