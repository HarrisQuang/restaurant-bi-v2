import numpy as np
import gspread as gs 
import pandas as pd 
import json 
from sqlalchemy import create_engine, text
from datetime import date
import os, sys
path = os.path.abspath('./controller')
sys.path.append(path)
import processing_data as proda

with open('config.json', "r", encoding='utf-8') as f:
    data = json.loads(f.read())

gc = gs.service_account(filename=data["gg_connect_file"])

engine = create_engine("postgresql://postgres:12345678@localhost:5432/demo_db")

def get_data_source_by_type(type):
    temp_arr = []
    ds_arr = data["data_source"]
    for el in ds_arr:
        if el['type'] == type:
            temp_arr.append(el)
    return temp_arr

def get_finance_data_term_list():  
    result = engine.execute("SELECT distinct ky FROM finance ORDER BY ky")
    result = result.fetchall()
    term_list = []
    for i in range(len(result)):
        term = 'THU CHI ' + str(result[i][0])
        term_list.append(term)
    return term_list

def get_current_term_finance_db():
    today = str(date.today())
    current_term = today[5:7] + '-' + today[2:4]
    result = engine.execute("SELECT ngay_number, ngay, doanh_thu FROM finance where ky = '%s'" % (current_term))
    df = pd.DataFrame(result.fetchall())
    return df

def get_current_term_order_db():
    today = str(date.today())
    current_term = today[5:7] + '-' + today[2:4]
    result = engine.execute("SELECT ngay_number, so_hoa_don FROM orders where ky = '%s'" % (current_term))
    df = pd.DataFrame(result.fetchall())
    return df

def get_order_data_term_list():  
    result = engine.execute("SELECT distinct cycle FROM resolve_overlap_dish_remove_extra_fee ORDER BY cycle")
    result = result.fetchall()
    term_list = []
    for i in range(len(result)):
        term = 'ORDER ' + str(result[i][0])
        term_list.append(term)
    return term_list

def get_order_data_dish_list():  
    result = engine.execute("SELECT distinct ten_mon FROM resolve_overlap_dish_remove_extra_fee ORDER BY ten_mon")
    result = result.fetchall()
    ten_mon_list = []
    for i in range(len(result)):
        ten_mon = str(result[i][0])
        ten_mon_list.append(ten_mon)
    ten_mon_list.append('...')
    return ten_mon_list

def get_order_data_from_db():
    result = engine.execute("SELECT * FROM orders")
    df = pd.DataFrame(result.fetchall())
    df['ngay'] = pd.to_datetime(df['ngay']).dt.date
    df.columns = data['completely_order_df_base_cols']
    return df

def get_resolve_overlap_remove_extra_data_from_db():
    result = engine.execute("SELECT * FROM resolve_overlap_dish_remove_extra_fee")
    df = pd.DataFrame(result.fetchall())
    df['ngay'] = pd.to_datetime(df['ngay']).dt.date
    df.columns = data['completely_resolve_overlap_remove_extra_df_base_cols']
    return df

def get_total_order_grouping_day():
    result = engine.execute("SELECT ngay_number, max(ngay) ngay, count(*) total_order FROM orders group by ngay_number")
    df = pd.DataFrame(result.fetchall())
    return df

def generate_total_sale_grouping_cycle_and_dish():
    result = engine.execute("SELECT cycle, ten_mon, max(don_gia) don_gia, sum(sl_ban) total_sl_ban, sum(sl_ban) * max(don_gia) dthu FROM resolve_overlap_dish_remove_extra_fee group by cycle, ten_mon order by cycle")
    df = pd.DataFrame(result.fetchall())
    return df

def get_total_sale_grouping_cycle_and_dish(cycle):
    cycle = cycle[6:]
    result = engine.execute(f"SELECT * FROM total_sale_grouping_cycle_dish where cycle = '{cycle}'")
    df = pd.DataFrame(result.fetchall())
    df.columns = ['Kỳ', 'Tên món', 'Đơn giá', 'SL bán', 'Doanh thu']
    return df, df.shape[0]
    
def get_vegan_day_data():
    result = engine.execute("SELECT * FROM vegan_day")
    df = pd.DataFrame(result.fetchall())
    return df

def get_existing_vegan_day():
    result = engine.execute("SELECT ngay_filter FROM orders_vegan_day")
    df = pd.DataFrame(result.fetchall())
    result = df['ngay_filter'].tolist()
    return result

def get_total_order_by_day(day_list):
    day_list = tuple(day_list)
    if len(day_list) == 1:
        result = engine.execute("SELECT ngay_number, ngay_filter, total_order FROM orders_vegan_day where ngay_filter = '%s'" % (day_list[0]))
    else:
        result = engine.execute("SELECT ngay_number, ngay_filter, total_order FROM orders_vegan_day where ngay_filter in %s" % (day_list,))
    df = pd.DataFrame(result.fetchall())
    df = df.sort_values(by = 'ngay_number', ascending = True)
    df = df[['ngay_filter', 'total_order']]
    df.columns = ['Ngày', 'Số đơn hàng']
    measure_delta = {'Số đơn hàng': '% Số đơn hàng'}
    df = proda.calculate_percentage_change(df, 'Ngày', measure_delta, grouping = False)
    return df

def get_statistic_dish_by_cycle_data_from_db(term, final_sltd_list):
    refactor_term = []
    total_count_term = len(get_order_data_term_list())
    for el in term:
        refactor_term.append(el[6:])
    refactor_term = tuple(refactor_term)
    final_sltd_list = tuple(final_sltd_list)
    if len(refactor_term) == total_count_term and len(final_sltd_list) == 1:
        result = engine.execute("SELECT * FROM statistic_dish_by_cycle where cycle in %s and ten_mon = '%s'" % (refactor_term, final_sltd_list[0]))
    elif len(refactor_term) == total_count_term and len(final_sltd_list) > 1:
        result = engine.execute("SELECT * FROM statistic_dish_by_cycle where cycle in %s and ten_mon in %s" % (refactor_term, final_sltd_list))
    elif len(refactor_term) > 1 and len(final_sltd_list) == 1:
        result = engine.execute("SELECT cycle, ten_mon, tong_sl_ban, max_sl_ban, min_sl_ban, avg_sl_ban, median_sl_ban, mode_sl_ban, cycle_number FROM statistic_dish_by_cycle where cycle in %s and ten_mon = '%s'" % (refactor_term, final_sltd_list[0]))
    elif len(refactor_term) > 1 and len(final_sltd_list) > 1:
        result = engine.execute("SELECT cycle, ten_mon, tong_sl_ban, max_sl_ban, min_sl_ban, avg_sl_ban, median_sl_ban, mode_sl_ban, cycle_number FROM statistic_dish_by_cycle where cycle in %s and ten_mon in %s" % (refactor_term, final_sltd_list))
    elif len(refactor_term) == 1 and len(final_sltd_list) == 1:
        result = engine.execute("SELECT cycle, ten_mon, tong_sl_ban, max_sl_ban, min_sl_ban, avg_sl_ban, median_sl_ban, mode_sl_ban, cycle_number FROM statistic_dish_by_cycle where cycle = '%s' and ten_mon = '%s'" % (refactor_term[0], final_sltd_list[0]))
    elif len(refactor_term) == 1 and len(final_sltd_list) > 1:
        result = engine.execute("SELECT cycle, ten_mon, tong_sl_ban, max_sl_ban, min_sl_ban, avg_sl_ban, median_sl_ban, mode_sl_ban, cycle_number FROM statistic_dish_by_cycle where cycle = '%s' and ten_mon in %s" % (refactor_term[0], final_sltd_list))
    df = pd.DataFrame(result.fetchall())
    if len(df.columns) == len(data['completely_statistic_dish_by_cycle_df_base_cols']):
        df.columns = data['completely_statistic_dish_by_cycle_df_base_cols']
    else:
        df.columns = data['completely_statistic_dish_by_cycle_df_base_cols'][:9]
        measure_delta = {'Tổng SL bán': '% Tổng SL bán', 'Max SL bán': '% Max SL bán', 'Min SL bán': '% Min SL bán',
                     'Avg SL bán': '% Avg SL bán', 'Median SL bán': '% Median SL bán', 'Mode SL bán': '% Mode SL bán'}
        df = proda.calculate_percentage_change(df, 'Tên món', measure_delta)
        df[["% Tổng SL bán", "% Max SL bán", "% Min SL bán", "% Avg SL bán", "% Median SL bán", "% Mode SL bán"]] = df[["% Tổng SL bán", "% Max SL bán", "% Min SL bán", "% Avg SL bán", "% Median SL bán", "% Mode SL bán"]].astype(str)
    return df

def get_cycle_list_by_type(type):
    res = get_data_source_by_type(type)
    temp_arr = []
    for el in res:
        year = el['Year']
        month = el['Month']
        string = month + '/' + year
        temp_arr.append(string)
    return temp_arr

def get_data_source_by_name(name):
    for el in data["data_source"]:
        if el['name'] == name:
            return el

def create_df_finance(file_name):
    try:
        df = pd.read_excel(f'temp/finance/{file_name}.xlsx', sheet_name='README')
    except:
        return None
    sheets = json.loads(df['sheets'].values[0])
    df_slice = json.loads(df['df_slice'].values[0])
    cols = json.loads(df['column_name'].values[0])
    type = str(df['type'].values[0])
    year = str(df['Year'].values[0])
    month = str(df['Month'].values[0])
    days = []
    for i in range(data["days_in_month"][month]):
        days.append(str(i + 1))
    if len(month) == 1:
        month = '0' + month
    add_term =  month + '-' + year[-2:]
    date = []
    term = []
    date_number = []
    for day in days:
        term.append(add_term)
        if len(day) == 1:
            tmp = '0'+ day + '/' + month + '/' + year
            tmp2 = year + month + '0'+ day
        else:
            tmp = day + '/' + month + '/' + year
            tmp2 = year + month + day
        date.append(tmp)
        date_number.append(tmp2)
    export_df = pd.DataFrame({'NGAY-NUMBER': date_number, 'KY': term, 'NGAY': date}) 
    for key in sheets:
        df1 = pd.read_excel(f'temp/finance/{file_name}.xlsx', sheet_name=int(sheets[key]), header=None)
        df2 = pd.DataFrame()
        for i in range(len(df_slice[sheets[key]]) - 1):
            df2[i+1] = df1.iloc[df_slice[sheets[key]][0][0]:df_slice[sheets[key]][0][1], df_slice[sheets[key]][i+1]]
        df2.columns = cols[sheets[key]]
        df2['NGAY'].replace('', np.nan, inplace=True)
        df2.dropna(subset = ['NGAY'], axis = 0, inplace=True)
        df2['NGAY'] = df2['NGAY'].apply(lambda x: x.strftime('%d/%m/%Y'))
        export_df = export_df.merge(df2, on = 'NGAY')
    final_report = {'year': year, 'month': month, 'type': type, 'df': export_df}
    return final_report

def create_df_order(file_name):
    try:
        df = pd.read_excel(f'temp/order/{file_name}.xlsx', sheet_name='README')
    except:
        return None
    type = str(df['type'].values[0])
    year = str(df['Year'].values[0])
    month = str(df['Month'].values[0])
    header_row_index = int(df['header_row_index'].values[0])
    df = pd.read_excel(f'temp/order/{file_name}.xlsx', sheet_name=0, header=None)
    df = df.iloc[header_row_index:]
    df.columns = df.iloc[0]
    df = df[2:]
    df = df[data['order_df_base_cols']]
    final_report = {'year': year, 'month': month, 'type': type, 'df': df}
    return final_report

def create_df_finance_old(ds):
    sheets = ds["detail"]["sheets"]
    df_slice = ds["detail"]["df_slice"]
    cols = ds["detail"]["column_name"]
    sh = gc.open_by_url(ds['link'])
    days = []
    for i in range(data["days_in_month"][ds["Month"]]):
        days.append(str(i + 1))
    type = ds["type"]
    year = ds["Year"]
    month = ds["Month"]
    date = []
    for day in days:
        if len(day) == 1:
            tmp = '0'+ day + '/' + month + '/' + year
        else:
            tmp = day + '/' + month + '/' + year
        date.append(tmp)
    export_df = pd.DataFrame({'NGAY': date}) 
    for key in sheets:
        ws = sh.get_worksheet(int(sheets[key]))
        df1 = pd.DataFrame(ws.get_values())
        df2 = pd.DataFrame()
        for i in range(len(df_slice[sheets[key]]) - 1):
            df2[i+1] = df1.iloc[df_slice[sheets[key]][0][0]:df_slice[sheets[key]][0][1], df_slice[sheets[key]][i+1]]
        df2.columns = cols[sheets[key]]
        export_df = export_df.merge(df2, on = 'NGAY')
    final_report = {'year': year, 'month': month, 'type': type, 'df': export_df}
    return final_report

def export_one_df(name):
    ds = get_data_source_by_name(name)
    if ds['type'] == 'finance':
        final_report = create_df_finance(ds)
    if ds['type'] == 'order':
        final_report = create_df_order(ds)
    return final_report

def export_list_df_finance(cycle):
    fn_arr = get_data_source_by_type('finance')
    final_df_arr = []
    for el in fn_arr:
        meta_report = create_df_finance(el)
        final_df_arr.append(meta_report)
    return final_df_arr

def export_list_df_order(cycle):
    if not cycle:
        temp = get_cycle_list_by_type('order')[-1]
        cycle =[temp]
    odr_arr = get_data_source_by_type('order')
    temp = []
    df = pd.DataFrame()
    for cy in cycle:
        i = cy.index('/')
        month = cy[0:i]
        year = cy[i+1:]
        for el in odr_arr:
            res = create_df_order(el)
            if res['year'] == year and res['month'] == month:
                order_df = res['df']
                temp.append(order_df)
                df = pd.concat(temp)
    return df

def export_list_df_by_type(cycle, type, function):
    if not cycle:
        temp = get_cycle_list_by_type(type)[-1]
        cycle =[temp]
    odr_arr = get_data_source_by_type(type)
    temp = []
    df = pd.DataFrame()
    for cy in cycle:
        i = cy.index('/')
        month = cy[0:i]
        year = cy[i+1:]
        for el in odr_arr:
            res = function(el)
            if res['year'] == year and res['month'] == month:
                order_df = res['df']
                temp.append(order_df)
                df = pd.concat(temp)
    return df

if __name__ == "__main__":
    res = create_df_order('ORDER 04-22')
    # print(res['df'].head())