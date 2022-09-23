import pandas as pd 
import numpy as np
from datetime import datetime 

# df = pd.read_excel('temp/THU CHI T5-22.xlsx', sheet_name=0)
# df = df.iloc[2:,[1,2]]
# df['TỔNG THU'].replace('', np.nan, inplace=True)
# df.dropna(subset = ['TỔNG THU'], axis = 0, inplace=True)
# df['TỔNG THU'] = df['TỔNG THU'].apply(lambda x: x.strftime('%d/%m/%Y'))

# print(df)
# print(df['TỔNG THU'])
# print(df.dtypes)

import json
df = pd.read_excel('temp/THU CHI T5-22.xlsx', sheet_name='README')
# res = json.loads(df['sheets'].values[0])
print(df['Month'].values[0])
print(type(str(df['Month'].values[0])))
# print(type(res['DOANH-THU-NGAY']))