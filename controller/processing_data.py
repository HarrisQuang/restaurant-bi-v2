import pandas as pd 
import numpy as np
from datetime import datetime
import json 
import os, sys
path = os.path.abspath('./controller')
sys.path.append(path)
from utils import *
from import_data import *

with open('config.json', "r") as f:
    data = json.loads(f.read())
    
def finalize_df_finance():
    base_cols = data['base_cols']
    result = export_df_finance()
    group = []
    for el in result:
        group.append(el['df'])
    final_df = pd.concat(group)
    for i in base_cols:
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

def group_stacked_bar_chart_finance():
    df = finalize_df_finance()
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
    
    
    