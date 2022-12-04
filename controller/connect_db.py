from sqlalchemy import create_engine
import pandas as pd
# name = 'Bo'
# details = '24 chui'
engine = create_engine("postgresql://postgres:12345678@localhost:5432/demo_db")

# engine.execute("CREATE TABLE IF NOT EXISTS finance (ngay_number integer PRIMARY KEY, ky text, ngay text, doanh_thu real, chi_phi real, net_sp_food real, grab real, baemin real, ck_sp_food real, sp_food real, ck_grab real, ck_baemin real, tai_quan real, pct_baemin real, pct_grab real, pct_sp_food real, pct_tai_quan real)")
# engine.execute("CREATE TABLE IF NOT EXISTS orders (ngay_number integer, ky text, ngay text, so_hoa_don text, ma_mon text, ten_mon text, sl_ban real, don_gia real, doanh_thu real)")

# "order_df_base_cols": ["Ngày", "Số hóa đơn", "Mã món", "Tên món", "SL bán", "Đơn giá", "Doanh thu"]
# engine.execute("CREATE TABLE IF NOT EXISTS records (name text PRIMARY KEY, details text)")
# engine.execute("INSERT INTO records (name,details) VALUES ('%s','%s')" % (name,details))

# name = 'new-dataset'
# ds = pd.DataFrame({'Name': ['Hai', 'Heo', 'Tin', 'Bo'], 'Age': [28, 18, 22, 28]})
# ds.to_sql('%s' % (name),engine,index=False,if_exists='replace',chunksize=1000)

"create table if not exists vegan_day (ngay_duong_number int, ngay_duong text, ngay_am text)"
"drop table if exists vegan_day"

'''
insert into vegan_day (ngay_duong, ngay_am) values ('2022-01-01', '2021-11-29'), ('2022-01-02', '2021-11-30'), ('2022-01-03', '2021-12-01'), ('2022-01-16', '2021-12-14'), ('2022-01-17', '2021-12-15'), ('2022-01-31', '2021-12-29'), ('2022-02-01', '2022-01-01');
insert into vegan_day (ngay_duong, ngay_am) values ('2022-02-14', '2022-01-14'), ('2022-02-15', '2022-01-15'), ('2022-03-01', '2022-01-29'), ('2022-03-02', '2022-01-30'), ('2022-03-03', '2022-02-01'), ('2022-03-16', '2022-02-14'), ('2022-03-17', '2022-02-15'), ('2022-03-31', '2022-02-29');
insert into vegan_day (ngay_duong, ngay_am) values ('2022-04-01', '2022-03-01'), ('2022-04-14', '2022-03-14'), ('2022-04-15', '2022-03-15'), ('2022-04-29', '2022-03-29'), ('2022-04-30', '2022-03-30'), ('2022-05-01', '2022-04-01'), ('2022-05-14', '2022-04-14'), ('2022-05-15', '2022-04-15');
insert into vegan_day (ngay_duong, ngay_am) values ('2022-05-29', '2022-04-29'), ('2022-05-30', '2022-05-01'), ('2022-06-12', '2022-05-14'), ('2022-06-13', '2022-05-15'), ('2022-06-27', '2022-05-29'), ('2022-06-28', '2022-05-30'), ('2022-06-29', '2022-06-01'), ('2022-07-12', '2022-06-14');
insert into vegan_day (ngay_duong, ngay_am) values ('2022-07-13', '2022-06-15'), ('2022-07-27', '2022-06-29'), ('2022-07-28', '2022-06-30'), ('2022-07-29', '2022-07-01'), ('2022-08-11', '2022-07-14'), ('2022-08-12', '2022-07-15'), ('2022-08-26', '2022-07-29'), ('2022-08-27', '2022-08-01');
insert into vegan_day (ngay_duong, ngay_am) values ('2022-09-09', '2022-08-14'), ('2022-09-10', '2022-08-15'), ('2022-09-24', '2022-08-29'), ('2022-09-25', '2022-08-30'), ('2022-09-26', '2022-09-01'), ('2022-10-09', '2022-09-14'), ('2022-10-10', '2022-09-15'), ('2022-10-24', '2022-09-29');
insert into vegan_day (ngay_duong, ngay_am) values ('2022-10-25', '2022-10-01'), ('2022-11-07', '2022-10-14'), ('2022-11-08', '2022-10-15'), ('2022-11-22', '2022-10-29'), ('2022-11-23', '2022-10-30'), ('2022-11-24', '2022-11-01'), ('2022-12-07', '2022-11-14'), ('2022-12-08', '2022-11-15');
insert into vegan_day (ngay_duong, ngay_am) values ('2022-12-22', '2022-11-29'), ('2022-12-23', '2022-12-01'), ('2023-01-05', '2022-12-14'), ('2023-01-06', '2022-12-15'), ('2023-01-20', '2022-12-29'), ('2022-01-21', '2022-12-30'), ('2023-01-22', '2023-01-01'), ('2023-02-04', '2022-01-14');
'''

""" 
create or replace function add_new_col_vegan_day()
returns trigger
LANGUAGE PLPGSQL
as $$
BEGIN
update vegan_day set ngay_duong_number = cast(concat(substring(ngay_duong, 1, 4), substring(ngay_duong, 6, 2), substring(ngay_duong, 9, 2)) as INTEGER);
RETURN new;
end;
$$
"""

"""
create trigger add_new_col_vegan_day
after insert
on vegan_day
FOR EACH STATEMENT
EXECUTE PROCEDURE add_new_col_vegan_day()
"""