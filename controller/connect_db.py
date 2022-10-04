from sqlalchemy import create_engine
import pandas as pd
# name = 'Bo'
# details = '24 chui'
engine = create_engine("postgresql://postgres:12345678@localhost:5432/demo_db")
# engine.execute("CREATE TABLE IF NOT EXISTS finance (ngay_number integer PRIMARY KEY, ky text, ngay text, doanh_thu real, chi_phi real, net_sp_food real, grab real, baemin real, ck_sp_food real, sp_food real, ck_grab real, ck_baemin real, tai_quan real, pct_baemin real, pct_grab real, pct_sp_food real, pct_tai_quan real)")
engine.execute("CREATE TABLE IF NOT EXISTS orders (ngay_number integer, ky text, ngay text, so_hoa_don text, ma_mon text, ten_mon text, sl_ban real, don_gia real, doanh_thu real)")

# "order_df_base_cols": ["Ngày", "Số hóa đơn", "Mã món", "Tên món", "SL bán", "Đơn giá", "Doanh thu"]
# engine.execute("CREATE TABLE IF NOT EXISTS records (name text PRIMARY KEY, details text)")
# engine.execute("INSERT INTO records (name,details) VALUES ('%s','%s')" % (name,details))

# name = 'new-dataset'
# ds = pd.DataFrame({'Name': ['Hai', 'Heo', 'Tin', 'Bo'], 'Age': [28, 18, 22, 28]})
# ds.to_sql('%s' % (name),engine,index=False,if_exists='replace',chunksize=1000)