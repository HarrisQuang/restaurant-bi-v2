import pandas as pd 
import numpy as np
from datetime import datetime
import json 
import os, sys
path = os.path.abspath('.')
sys.path.append(path)
from controller.utils import *
from controller.get_data import *
from sqlalchemy import create_engine, text

with open('config.json', "r", encoding='utf-8') as f:
    data = json.loads(f.read())

msk_dish_name = data['mask_dish_name']

engine = create_engine("postgresql://postgres:12345678@localhost:5432/demo_db")

def processing_df_finance(final_df):
    finance_df_base_cols = data['finance_df_base_cols']
    for i in finance_df_base_cols:
        final_df[i].replace(np.nan, '0', inplace=True)
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

def processing_df_finance_old(final_df):
    finance_df_base_cols = data['finance_df_base_cols']
    for i in finance_df_base_cols:
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

def resolve_various_dish_name(df):
    df['T??n m??n'] = df['T??n m??n'].str.strip()
    vro_dish_name = data['various_dish_name']
    for key in vro_dish_name.keys():
        old_val = replace_double_quote(key)
        new_val = replace_double_quote(vro_dish_name[key])
        df['T??n m??n'].replace(old_val, new_val, inplace=True)
    return df

def create_mask_dish_name(df):
    df['T??n m??n'] = df['T??n m??n'].str.strip()
    for key in msk_dish_name.keys():
        old_val = replace_double_quote(key)
        new_val = replace_double_quote(msk_dish_name[key])
        df['T??n m??n'].replace(old_val, new_val, inplace=True)
    return df

def resolve_overlap_dish_remove_extra_fee(df):
    df = resolve_various_dish_name(df)
    for i in data['extra_fee']:
        df = df[df['T??n m??n'] != i]
    df = df.groupby(['Ng??y', 'Cycle', 'M?? m??n', 'T??n m??n'], as_index=False).agg({'SL b??n': 'sum', 'Doanh thu': 'sum', 
                                                                        '????n gi??': 'max'})
    for k in df['Ng??y'].unique():
        for i, mon in enumerate(data['overlap_dish_code']):
            don_gia = 0
            sl_ban = 0
            doanh_thu = 0
            cycle = ''
            flag = 0
            for j in mon:
                try:
                    don_gia = df[(df['M?? m??n'] == j) & (df['Ng??y'] == k)]['????n gi??'].values[0]
                    cycle = df[(df['M?? m??n'] == j) & (df['Ng??y'] == k)]['Cycle'].values[0]
                    sl_ban += df[(df['M?? m??n'] == j) & (df['Ng??y'] == k)]['SL b??n'].values[0]
                    doanh_thu += df[(df['M?? m??n'] == j) & (df['Ng??y'] == k)]['Doanh thu'].values[0]
                    df.loc[(df['M?? m??n'] == j) & (df['Ng??y'] == k), 'M?? m??n'] = np.nan
                    flag += 1
                except:
                    pass   
            if flag > 0:
                new_row = pd.DataFrame({'Ng??y': k, 'Cycle': cycle, 'T??n m??n': data['new_dish_name'][i], 'M?? m??n': '', 'SL b??n' : sl_ban, '????n gi??': don_gia,
                        'Doanh thu': doanh_thu}, index = [df.shape[0]])
            # new_row = {'Ng??y': k, 'T??n m??n': data['new_dish_name'][i], 'M?? m??n': '', 'SL b??n' : sl_ban, '????n gi??': don_gia,
            #         'Doanh thu': doanh_thu}
                df = pd.concat([new_row, df.loc[:]])
            # df = df.append(new_row, ignore_index=True)
                df.dropna(subset=['M?? m??n'], axis = 0, inplace = True)
    df['ngay_number'] = df['Ng??y'].apply(lambda x: int(x.strftime('%Y%m%d')))
    # create mask disk name
    # df = create_mask_dish_name(df) 
    return df

def processing_df_order(final_df):
    final_df['M?? m??n'].replace('', np.nan, inplace=True)
    final_df.dropna(subset = ['M?? m??n'], axis = 0, inplace=True)
    final_df['Ng??y'].replace('', np.nan, inplace=True)
    final_df.dropna(subset = ['Ng??y'], axis = 0, inplace=True)
    final_df['SL b??n'] = final_df['SL b??n'].astype(float, copy=True)
    final_df[['????n gi??', 'Doanh thu']] = final_df[['????n gi??', 'Doanh thu']].astype(float, copy=True)
    final_df['Ng??y'] = pd.to_datetime(final_df['Ng??y']).dt.date
    final_df['Cycle'] = final_df['Ng??y'].apply(lambda x: str(x.month) + '-' + str(x.year)[2:])
    final_df['Cycle'] = final_df['Cycle'].apply(lambda x: '0'+ x if len(x) == 4 else x)
    final_df['ngay_number'] = final_df['Ng??y'].apply(lambda x: x.strftime('%Y%m%d')).astype(int, copy=True)
    final_df = final_df[data['completely_order_df_base_cols']]
    return final_df

def export_one_df_finance(file_name):
    final_df = create_df_finance(file_name)
    if final_df is not None:
        final_df = processing_df_finance(final_df['df'])
    return final_df

def export_one_df_order(file_name):
    final_df = create_df_order(file_name)
    if final_df is not None:
        final_df = processing_df_order(final_df['df'])
    return final_df
    
def finalize_one_df_finance_by_term(term):
    term = term[8:]
    result = engine.execute("SELECT * FROM finance WHERE ky = '%s'" % (term))
    df = pd.DataFrame(result.fetchall())
    df.columns = data['completely_finance_df_base_cols']
    return df

def finalize_one_df_order_by_term(term):
    term = term[6:]
    result = engine.execute("SELECT * FROM orders WHERE ky = '%s'" % (term))
    df = pd.DataFrame(result.fetchall())
    df['ngay'] = pd.to_datetime(df['ngay']).dt.date
    df.columns = data['completely_order_df_base_cols']
    return df

def finalize_list_df_finance_by_cycle(cycle):
    df = export_list_df_by_type(cycle, 'finance', create_df_finance)
    df = processing_df_finance(df)
    return df

def finalize_list_df_order_by_cycle(cycle):
    df = export_list_df_by_type(cycle, 'order', create_df_order)
    df = processing_df_order(df)
    return df

def finalize_list_df_order_by_term(term):
    if not term:
        temp = get_order_data_term_list()[-1]
        term =[temp]
    part_df = []
    for el in term:
        el = el[6:]
        result = engine.execute("SELECT * FROM orders WHERE ky = '%s'" % (el))
        df = pd.DataFrame(result.fetchall())
        df['ngay'] = pd.to_datetime(df['ngay']).dt.date
        df.columns = data['completely_order_df_base_cols']
        part_df.append(df)
    final_df = pd.concat(part_df, axis = 0)
    return final_df

def calculate_percentage_change(df, orgin, criteria, grouping = True):
    temp = []
    if grouping == True:
        for i, val in enumerate(df[orgin]):
            if i == 0:
                temp.append(0)
            else:
                if df[orgin][i] != df[orgin][i-1]:
                    temp.append(0)
                else:
                    temp.append(1)
    else:
        for i, val in enumerate(df[orgin]):
            if i == 0:
                temp.append(0)
            else:
                temp.append(1)
    df['flag'] = temp
    for el in criteria.keys():
        temp = []
        for i, el1 in enumerate(df[el]):
            if df['flag'][i] == 0:
                temp.append('0')
            else:
                delta = round((df[el][i] - df[el][i-1])/df[el][i-1]*100, 2)
                temp.append(delta)
        df[criteria[el]] = temp
    df = df.drop('flag', axis = 1)
    return df

def generate_total_order_vegan_day():
    total_order_grouping_day = get_total_order_grouping_day()
    vegan_day_list = get_vegan_day_data()
    total_order_vegan_day = total_order_grouping_day.merge(vegan_day_list, left_on = 'ngay_number', right_on = 'ngay_duong_number')
    total_order_vegan_day['ngay_filter'] = total_order_vegan_day['ngay_duong'].apply(lambda x: x[8:10]) + '/' + total_order_vegan_day['ngay_duong'].apply(lambda x: x[5:7]) + '/' + total_order_vegan_day['ngay_duong'].apply(lambda x: x[2:4]) + '(' + total_order_vegan_day['ngay_am'].apply(lambda x: x[8:10]) + '/' + total_order_vegan_day['ngay_am'].apply(lambda x: x[5:7]) + ')'
    total_order_vegan_day = total_order_vegan_day.sort_values(by='ngay_number')
    return total_order_vegan_day 

def statistic_dish_by_cycle(df):
    part_df = []
    count = 0
    for c in df['Cycle'].unique():
        for m in df['T??n m??n'].unique():
            temp_df = df[(df['Cycle'] == c) & (df['T??n m??n'] == m)]
            if not temp_df.empty:
                sum_sl_ban = np.sum(temp_df['SL b??n'])
                max_sl_ban = np.max(temp_df['SL b??n'])
                min_sl_ban = np.min(temp_df['SL b??n'])
                avg_sl_ban = round(np.mean(temp_df['SL b??n']),2)
                median_sl_ban = np.median(temp_df['SL b??n'])
                vals, counts = np.unique(temp_df['SL b??n'], return_counts=True)
                mode_value_index = np.argwhere(counts == np.max(counts))
                mode_sl_ban = vals[mode_value_index][0][0]
                temp_df = pd.DataFrame({'Cycle': c, 'T??n m??n': m, 'T???ng SL b??n': sum_sl_ban, 'Max SL b??n': max_sl_ban,
                                    'Min SL b??n': min_sl_ban, 'Avg SL b??n': avg_sl_ban, 'Median SL b??n': median_sl_ban,
                                    'Mode SL b??n': mode_sl_ban}, index=[count])
                part_df.append(temp_df)
                count += 1
    df = pd.concat(part_df, axis=0)
    df['Cycle_number'] = df['Cycle'].apply(lambda x: int(x[3:] + x[0:2]))
    df = df.sort_values(by = ['T??n m??n', 'Cycle_number'], ascending = True).reset_index(drop = True)
    measure_delta = {'T???ng SL b??n': '% T???ng SL b??n', 'Max SL b??n': '% Max SL b??n', 'Min SL b??n': '% Min SL b??n',
                     'Avg SL b??n': '% Avg SL b??n', 'Median SL b??n': '% Median SL b??n', 'Mode SL b??n': '% Mode SL b??n'}
    df = calculate_percentage_change(df, 'T??n m??n', measure_delta)
    return df

def markup_percentage_change(df, markup_cols):
    for el in markup_cols:
        temp = []
        df[el] = df[el].astype(str)
        for val in df[el].values:
            if float(val) > 0:
                val = '???? ' + val + '%'
                temp.append(val)
            elif float(val) == 0:
                val = '???? ' + val + '%'
                temp.append(val)
            else:
                val = '???? ' + val + '%'
                temp.append(val)
        df[el] = temp
    return df
        
def markup_statistic_dish_by_cycle(df):
    markup_cols = data['completely_statistic_dish_by_cycle_df_base_cols'][10:]
    df = markup_percentage_change(df, markup_cols)
    return df

def markup_total_order_vegan_day(df):
    markup_cols = ['% S??? ????n h??ng']
    df = markup_percentage_change(df, markup_cols)
    return df

def finalize_list_df_order_grouping_cycle(df, sltd_list):
    df = resolve_overlap_dish_remove_extra_fee(df)
    filter_df = []
    for i in sltd_list:
        if i != '...':
            temp = df[df['T??n m??n'] == i ]
            filter_df.append(temp)
    if filter_df:
        final_df = pd.concat(filter_df, axis=0)
    else:
        final_df = df[df['T??n m??n'] == 'B??n Th??i']
    part_df = []
    count = 0
    for c in final_df['Cycle'].unique():
        for m in final_df['T??n m??n'].unique():
            temp_df = final_df[(final_df['Cycle'] == c) & (final_df['T??n m??n'] == m)]
            sum_sl_ban = np.sum(temp_df['SL b??n'])
            max_sl_ban = np.max(temp_df['SL b??n'])
            min_sl_ban = np.min(temp_df['SL b??n'])
            avg_sl_ban = round(np.mean(temp_df['SL b??n']),2)
            median_sl_ban = np.median(temp_df['SL b??n'])
            vals, counts = np.unique(temp_df['SL b??n'], return_counts=True)
            mode_value_index = np.argwhere(counts == np.max(counts))
            mode_sl_ban = vals[mode_value_index][0][0]
            temp_df = pd.DataFrame({'Cycle': c, 'T??n m??n': m, 'T???ng SL b??n': sum_sl_ban, 'Max SL b??n': max_sl_ban,
                                 'Min SL b??n': min_sl_ban, 'Avg SL b??n': avg_sl_ban, 'Median SL b??n': median_sl_ban,
                                 'Mode SL b??n': mode_sl_ban}, index=[count])
            part_df.append(temp_df)
            count += 1
    final_df = pd.concat(part_df, axis=0)
    return final_df

def calculate_delta_measure_dish_by_cycle(df):
    df['Cycle_number'] = df['Cycle'].apply(lambda x: int(x[3:] + x[0:2]))
    df = df.sort_values(by = ['T??n m??n', 'Cycle_number'], ascending = True).reset_index(drop = True)
    measure_delta = {'T???ng SL b??n': '% T???ng SL b??n', 'Max SL b??n': '% Max SL b??n', 'Min SL b??n': '% Min SL b??n',
                     'Avg SL b??n': '% Avg SL b??n', 'Median SL b??n': '% Median SL b??n', 'Mode SL b??n': '% Mode SL b??n'}
    temp = []
    for i, val in enumerate(df['T??n m??n']):
        if i == 0:
            temp.append(0)
        else:
            if df['T??n m??n'][i] != df['T??n m??n'][i-1]:
                temp.append(0)
            else:
                temp.append(1)
    df['flag'] = temp
    for el in measure_delta.keys():
        temp = []
        for i, el1 in enumerate(df[el]):
            if df['flag'][i] == 0:
                temp.append('0%')
            else:
                delta = round((df[el][i] - df[el][i-1])/df[el][i-1]*100, 2)
                if delta > 0:
                    delta = '???? ' + str(delta) + '%'
                    temp.append(delta)
                else:
                    delta = '???? ' + str(delta) + '%'
                    temp.append(delta)
        df[measure_delta[el]] = temp
    return df
    
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
    dthu_type = ['BAEMIN', 'GRAB', 'SP-FOOD', 'T???i qu??n']
    return date_from, date_to, dthu_type

def percent_revenue_from_source(df, ds, de, options):
    df = df[['PCT-BAEMIN', 'PCT-GRAB', 'PCT-SP-FOOD', 'PCT-TAI-QUAN', 'NGAY']]
    df.columns = ['BAEMIN', 'GRAB', 'SP-FOOD', 'T???i qu??n', 'Ng??y']
    df = df.melt(id_vars=['Ng??y'], var_name=['Ngu???n-doanh-thu'], value_name='T???-l???-%')
    df['Ng??y'] = pd.to_datetime(df['Ng??y']).dt.date
    df = df[(df['Ng??y'] >= ds) & (df['Ng??y'] <= de)]
    filter_df = []
    for i in options:
        temp = df[df['Ngu???n-doanh-thu'] == i]
        filter_df.append(temp)
    if filter_df:
        final_df = pd.concat(filter_df, axis=0)
    else:
        final_df = df
    final_df['Ng??y'] = final_df['Ng??y'].apply(lambda x: x.strftime('%d/%m/%Y'))
    return final_df

def percent_revenue_from_source_by_cycle(df, cycle, options):
    df = df[['PCT-BAEMIN', 'PCT-GRAB', 'PCT-SP-FOOD', 'PCT-TAI-QUAN', 'NGAY']]
    df.columns = ['BAEMIN', 'GRAB', 'SP-FOOD', 'T???i qu??n', 'Ng??y']
    df = df.melt(id_vars=['Ng??y'], var_name=['Ngu???n-doanh-thu'], value_name='T???-l???-%')
    df['Ng??y'] = pd.to_datetime(df['Ng??y']).dt.date
    df = df[(df['Ng??y'] >= ds) & (df['Ng??y'] <= de)]
    filter_df = []
    for i in options:
        temp = df[df['Ngu???n-doanh-thu'] == i]
        filter_df.append(temp)
    if filter_df:
        final_df = pd.concat(filter_df, axis=0)
    else:
        final_df = df
    final_df['Ng??y'] = final_df['Ng??y'].apply(lambda x: x.strftime('%d/%m/%Y'))
    return final_df

def create_df_stt_prfs(df, options):
    temp_arr = []
    for i, el in enumerate(options):
        row = [np.min(df['Ng??y']), np.max(df['Ng??y']), options[i], round(np.max(df[df['Ngu???n-doanh-thu'] == options[i]]['T???-l???-%']), 2),
                    round(np.min(df[df['Ngu???n-doanh-thu'] == options[i]]['T???-l???-%']), 2), round(np.mean(df[df['Ngu???n-doanh-thu'] == options[i]]['T???-l???-%']), 2)]
        temp_arr.append(row)
    temp_df = pd.DataFrame(temp_arr, columns=['Ng??y b???t ?????u', 'Ng??y k???t th??c', 'Lo???i doanh thu', 'Max (T??? l??? %)', 'Min (T??? l??? %)', "Avg (T??? l??? %)"])
    return temp_df

def get_statistic_prfs(df, options):
    if len(options):
        final_df = create_df_stt_prfs(df, options)
    else:
        options = ['BAEMIN', 'GRAB', 'SP-FOOD', 'T???i qu??n']
        final_df = create_df_stt_prfs(df, options)
    return final_df

def get_default_params_bsd(df):
    date_from = np.min(df['Ng??y'])
    date_to = np.max(df['Ng??y'])
    return date_from, date_to

def top_slider(df, ds, de):
    df = resolve_overlap_dish_remove_extra_fee(df)
    df = df[(df['Ng??y'] >= ds) & (df['Ng??y'] <= de)]
    df = df.groupby(["T??n m??n", 'M?? m??n'], as_index=False).agg({'????n gi??': 'max', 'SL b??n': 'sum', 
                                                                            'Doanh thu': 'sum'})
    return df, df.shape[0]

def sort_df(df, by, asc=False):
    df = df.sort_values(by = by, ascending = asc).reset_index(drop = True)
    df.index += 1
    return df 

def top_seller_dish(df, top_quantity, top_revenue):
    top_dish_quantity = df.sort_values(by='SL b??n', ascending=False).head(top_quantity).reset_index(drop = True)
    top_dish_quantity.index += 1
    top_dish_quantity = top_dish_quantity[['T??n m??n', '????n gi??', 'Doanh thu', 'SL b??n']]
    top_dish_revenue = df.sort_values(by='Doanh thu', ascending=False).head(top_revenue).reset_index(drop = True)
    top_dish_revenue.index += 1
    top_dish_revenue = top_dish_revenue[['T??n m??n', '????n gi??', 'SL b??n', 'Doanh thu']]
    return top_dish_quantity, top_dish_revenue

def top_seller_dish_through_cycle(df, top_quantity, top_revenue):
    top_dish_quantity = df.sort_values(by='SL b??n', ascending=False).head(top_quantity).reset_index(drop = True)
    top_dish_quantity.index += 1
    top_dish_quantity = top_dish_quantity[['K???', 'T??n m??n', '????n gi??', 'Doanh thu', 'SL b??n']]
    top_dish_revenue = df.sort_values(by='Doanh thu', ascending=False).head(top_revenue).reset_index(drop = True)
    top_dish_revenue.index += 1
    top_dish_revenue = top_dish_revenue[['K???', 'T??n m??n', '????n gi??', 'SL b??n', 'Doanh thu']]
    return top_dish_quantity, top_dish_revenue

def dish_list(df):
    df = resolve_overlap_dish_remove_extra_fee(df)
    dish_list = df['T??n m??n'].unique().tolist()
    dish_list.sort()
    dish_list.append('...')
    return dish_list

def dish_sale_every_day(df, sltd_list):
    df = resolve_overlap_dish_remove_extra_fee(df)
    filter_df = []
    for i in sltd_list:
        if i != '...':
            temp = df[df['T??n m??n'] == i ]
            filter_df.append(temp)
    if filter_df:
        final_df = pd.concat(filter_df, axis=0)
    else:
        final_df = df[(df['T??n m??n'] == 'C??m tr???n') | (df['T??n m??n'] == 'G???i Cu???n N???m') |
                      (df['T??n m??n'] == 'B??n Ch??? Gi?? N???m') | (df['T??n m??n'] == 'B??n Th??i')]
        # final_df = df[(df['T??n m??n'] == msk_dish_name['C??m tr???n']) | (df['T??n m??n'] == msk_dish_name['G???i Cu???n N???m']) |
        #               (df['T??n m??n'] == msk_dish_name['B??n Ch??? Gi?? N???m']) | (df['T??n m??n'] == msk_dish_name['B??n Th??i'])]
    final_df.loc[:,'Ng??y'] = final_df['Ng??y'].apply(lambda x: x.strftime('%d/%m/%Y'))
    count_ngay = final_df['Ng??y'].nunique()
    return (count_ngay, final_df)

def create_df_stt_dsed(df, sltd_list):
    df = resolve_overlap_dish_remove_extra_fee(df)
    temp_arr = []
    for i, el in enumerate(sltd_list):
        if el != '...':
            new_df = df[df['T??n m??n'] == sltd_list[i]]
            vals, counts = np.unique(new_df['SL b??n'], return_counts=True)
            mode_value_index = np.argwhere(counts == np.max(counts))
            mode_value = vals[mode_value_index][0][0]
            row = [np.min(df['Ng??y']), np.max(df['Ng??y']), sltd_list[i], round(np.max(df[df['T??n m??n'] == sltd_list[i]]['SL b??n']), 2),
                        round(np.min(df[df['T??n m??n'] == sltd_list[i]]['SL b??n']), 2), 
                        round(np.mean(df[df['T??n m??n'] == sltd_list[i]]['SL b??n']), 2),
                        round(np.median(df[df['T??n m??n'] == sltd_list[i]]['SL b??n']), 2),
                        round(mode_value, 2)]
            temp_arr.append(row)
    temp_df = pd.DataFrame(temp_arr, columns=['Ng??y b???t ?????u', 'Ng??y k???t th??c', 'M??n', 'Max (SL b??n)', 'Min (SL b??n)', "Avg (SL b??n)",
                                              'Median (SL b??n)', 'Mode (SL b??n)'])
    return temp_df

def get_statistic_dsed(df, sltd_list):
    count = 0
    for i in sltd_list:
        if i == '...':
            count += 1
    if count < 5:
        final_df = create_df_stt_dsed(df, sltd_list)
    else:
        sltd_list = ['C??m tr???n', 'G???i Cu???n N???m', 'B??n Ch??? Gi?? N???m', 'B??n Th??i']
        # sltd_list = [msk_dish_name['C??m tr???n'], msk_dish_name['G???i Cu???n N???m'], msk_dish_name['B??n Ch??? Gi?? N???m'], msk_dish_name['B??n Th??i']]
        final_df = create_df_stt_dsed(df, sltd_list)
    return final_df

def order_sale_every_day(df):
    arr = []
    for el in df['Ng??y'].unique():
        count = df[df['Ng??y'] == el]['S??? h??a ????n'].nunique()
        temp = [el, count]
        arr.append(temp)
    df = pd.DataFrame(arr, columns=['Ng??y', 'SL h??a ????n'])
    df['Ng??y'] = df['Ng??y'].apply(lambda x: x.strftime('%d/%m/%Y'))
    return df

def get_statistic_osed(df):
    temp_arr = []
    vals, counts = np.unique(df['SL h??a ????n'], return_counts=True)
    mode_value_index = np.argwhere(counts == np.max(counts))
    mode_value = vals[mode_value_index][0][0]
    row = [round(np.max(df['SL h??a ????n']), 2), round(np.min(df['SL h??a ????n']), 2), round(np.mean(df['SL h??a ????n']), 2),
           round(np.median(df['SL h??a ????n']), 2), round(mode_value, 2)]
    temp_arr.append(row)
    final_df = pd.DataFrame(temp_arr, columns=['Max (SL h??a ????n)', 'Min (SL h??a ????n)', "Avg (SL h??a ????n)",
                                               'Median (SL h??a ????n)', 'Mode (SL h??a ????n)'])
    return final_df

if __name__ == "__main__":
    # df = export_one_df_order('ORDER 04-22')
    df = export_one_df_finance('THU CHI 05-22')
    # print(df.head(10))
    # print(df.dtypes)
    