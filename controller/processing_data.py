import pandas as pd 
import numpy as np
from datetime import datetime
import json 
import os, sys
path = os.path.abspath('./controller')
sys.path.append(path)
from utils import *
from get_data import *

with open('config.json', "r") as f:
    data = json.loads(f.read())

def processing_final_df_finance(final_df):
    finance_order_base_cols = data['finance_order_base_cols']
    for i in finance_order_base_cols:
        final_df[i] = transform_col(final_df[i])
        final_df[i] = final_df[i].astype(float)
    final_df['SP-FOOD'] = final_df['CK-SP-FOOD'] + final_df['NET-SP-FOOD']
    final_df['CK-GRAB'] = final_df['GRAB'] * 25/100
    final_df['CK-BAEMIN'] = final_df['BAEMIN'] * 25/100
    final_df['TAI-QUAN'] = final_df['DOANH-THU'] - final_df['GRAB'] - final_df['BAEMIN'] - final_df['SP-FOOD']
    final_df['PCT-BAEMIN'] = round(final_df['BAEMIN']/(final_df['BAEMIN'] + final_df['GRAB'] + final_df['SP-FOOD'] + final_df['TAI-QUAN'])*100, 2)
    final_df['PCT-GRAB'] = round(final_df['GRAB']/(final_df['BAEMIN'] + final_df['GRAB'] + final_df['SP-FOOD'] + final_df['TAI-QUAN'])*100, 2)
    final_df['PCT-SP-FOOD'] = round(final_df['SP-FOOD']/(final_df['BAEMIN'] + final_df['GRAB'] + final_df['SP-FOOD'] + final_df['TAI-QUAN'])*100, 2) 
    final_df['PCT-TAI-QUAN'] = round(final_df['TAI-QUAN']/(final_df['BAEMIN'] + final_df['GRAB'] + final_df['SP-FOOD'] + final_df['TAI-QUAN'])*100, 2)
    return final_df

def finalize_list_df_finance():
    result = export_df_finance()
    group = []
    for el in result:
        group.append(el['df'])
    final_df = pd.concat(group)
    final_df = processing_final_df_finance(final_df)
    return final_df

def finalize_one_df_finance(name):
    final_df = export_one_df(name)['df']
    final_df = processing_final_df_finance(final_df)
    return final_df

def revenue_cost_overal(df):
    df = df[['BAEMIN', 'GRAB', 'SP-FOOD', 'TAI-QUAN', 'CHI-PHI', 'CK-SP-FOOD', 'CK-GRAB', 'CK-BAEMIN', 'NGAY']]
    df = df.melt(id_vars=['NGAY'], var_name=['Sub-cate'], value_name='giatri')
    temp = []
    for i in df['Sub-cate']:
        if i == 'BAEMIN' or i == 'GRAB' or i == 'SP-FOOD' or i == 'TAI-QUAN':
            temp.append('DOANH-THU')
        else:
            temp.append('CHI-PHI')
    df['Main-cate'] = temp
    df = df[['NGAY', 'Main-cate', 'Sub-cate', 'giatri']]
    agg_new_df = df.groupby(['NGAY', 'Main-cate'])['giatri'].agg(np.sum).reset_index()
    temp = []
    for id, row in df.iterrows():
        val = agg_new_df[(agg_new_df['NGAY'] == row['NGAY']) & (agg_new_df['Main-cate'] == row['Main-cate'])]['giatri'].values[0]
        temp.append(val)
    df['total'] = temp
    df['perctg'] = round(df['giatri']/df['total'] * 100,2)
    df['format-perctg'] = df['perctg'].apply(lambda x: f"{x} %")
    df['format-giatri'] = df['giatri'].apply(lambda x: f"{x:,.0f} VND")
    return df

def get_default_params_prfs(df):
    df['NGAY'] = df['NGAY'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y'))
    df['NGAY'] = pd.to_datetime(df['NGAY']).dt.date
    date_from = np.min(df['NGAY'])
    date_to = np.max(df['NGAY'])
    dthu_type = ['BAEMIN', 'GRAB', 'SP-FOOD', 'Tại quán']
    return date_from, date_to, dthu_type

def percent_revenue_from_source(df, ds, de, options):
    df = df[['PCT-BAEMIN', 'PCT-GRAB', 'PCT-SP-FOOD', 'PCT-TAI-QUAN', 'NGAY']]
    df.columns = ['BAEMIN', 'GRAB', 'SP-FOOD', 'Tại quán', 'Ngày']
    df = df.melt(id_vars=['Ngày'], var_name=['Nguồn-doanh-thu'], value_name='Tỷ-lệ-%')
    df['Ngày'] = pd.to_datetime(df['Ngày']).dt.date
    df = df[(df['Ngày'] >= ds) & (df['Ngày'] <= de)]
    filter_df = []
    for i in options:
        temp = df[df['Nguồn-doanh-thu'] == i]
        filter_df.append(temp)
    if filter_df:
        final_df = pd.concat(filter_df, axis=0)
    else:
        final_df = df
    final_df['Ngày'] = final_df['Ngày'].apply(lambda x: x.strftime('%d/%m/%Y'))
    return final_df

def create_df_stt_prfs(df, options):
    temp_arr = []
    for i, el in enumerate(options):
        row = [np.min(df['Ngày']), np.max(df['Ngày']), options[i], round(np.max(df[df['Nguồn-doanh-thu'] == options[i]]['Tỷ-lệ-%']), 2),
                    round(np.min(df[df['Nguồn-doanh-thu'] == options[i]]['Tỷ-lệ-%']), 2), round(np.mean(df[df['Nguồn-doanh-thu'] == options[i]]['Tỷ-lệ-%']), 2)]
        temp_arr.append(row)
    temp_df = pd.DataFrame(temp_arr, columns=['Ngày bắt đầu', 'Ngày kết thúc', 'Loại doanh thu', 'Max', 'Min', "Avg"])
    return temp_df

def get_statistic_prfs(df, options):
    if len(options):
        final_df = create_df_stt_prfs(df, options)
    else:
        options = ['BAEMIN', 'GRAB', 'SP-FOOD', 'Tại quán']
        final_df = create_df_stt_prfs(df, options)
    return final_df