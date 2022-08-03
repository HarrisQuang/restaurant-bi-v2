import numpy as np
import gspread as gs 
import pandas as pd 
import json 

with open('config.json', "r", encoding='utf-8') as f:
    data = json.loads(f.read())

gc = gs.service_account(filename=data["gg_connect_file"])

def get_data_source_by_type(type):
    temp_arr = []
    ds_arr = data["data_source"]
    for el in ds_arr:
        if el['type'] == type:
            temp_arr.append(el)
    return temp_arr

def get_finance_data_name_list():
    res = get_data_source_by_type('finance')
    temp_arr = []
    for el in res:
        temp_arr.append(el['name'])
    return temp_arr

def get_order_data_name_list():
    res = get_data_source_by_type('order')
    temp_arr = []
    for el in res:
        temp_arr.append(el['name'])
    return temp_arr

def get_data_source_by_name(name):
    for el in data["data_source"]:
        if el['name'] == name:
            return el

def create_df_finance(ds):
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

def create_df_order(ds):
    type = ds["type"]
    year = ds["Year"]
    month = ds["Month"]
    sh = gc.open_by_url(ds['link'])
    ws = sh.get_worksheet(0)
    df = pd.DataFrame(ws.get_values())
    df = df.iloc[ds['header_row_index']:]
    df.columns = df.iloc[0]
    df = df[2:]
    df = df[data['order_df_base_cols']]
    final_report = {'year': year, 'month': month, 'type': type, 'df': df}
    return final_report

def export_one_df(name):
    ds = get_data_source_by_name(name)
    if ds['type'] == 'finance':
        final_report = create_df_finance(ds)
    if ds['type'] == 'order':
        final_report = create_df_order(ds)
    return final_report

def export_df_finance():
    fn_arr = get_data_source_by_type('finance')
    final_df_arr = []
    for el in fn_arr:
        sheets = el["detail"]["sheets"]
        df_slice = el["detail"]["df_slice"]
        cols = el["detail"]["column_name"]
        sh = gc.open_by_url(el['link'])
        days = []
        for i in range(data["days_in_month"][el["Month"]]):
            days.append(str(i + 1))
        type = el["type"]
        year = el["Year"]
        month = el["Month"]
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
        final_df_arr.append(final_report)
    return final_df_arr

def export_df_order():
    odr_arr = get_data_source_by_type('order')
    final_df_arr = []
    for el in odr_arr:
        type = el["type"]
        year = el["Year"]
        month = el["Month"]
        df = pd.read_excel(el['link'], sheet_name=0, header=8)
        df = df[['Ngày', 'Số hóa đơn', 'Mã món', 'Tên món', 'SL bán', 'Đơn giá', 'Doanh thu']]
        final_report = {'year': year, 'month': month, 'type': type, 'df': df}
        final_df_arr.append(final_report)
    return final_df_arr


# ds = export_one_df('Báo cáo đơn hàng tháng 4/22')
# print(ds['df'].head())