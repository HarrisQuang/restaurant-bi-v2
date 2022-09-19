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
    df = pd.read_excel(f'temp/{file_name}.xlsx', sheet_name='README')
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
    date = []
    for day in days:
        if len(day) == 1:
            tmp = '0'+ day + '/' + month + '/' + year
        else:
            tmp = day + '/' + month + '/' + year
        date.append(tmp)
    export_df = pd.DataFrame({'NGAY': date}) 
    for key in sheets:
        df1 = pd.read_excel(f'temp/{file_name}.xlsx', sheet_name=int(sheets[key]))
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
    df = create_df_finance('THU CHI T5-22')['df']
    print(df.head())