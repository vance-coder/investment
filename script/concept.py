import akshare as ak
import json
import pandas as pd
from datetime import datetime

data = {}

df_list = []

stock_board_concept_name_ths_df = ak.stock_board_concept_name_ths()

for idx, row in stock_board_concept_name_ths_df.iterrows():
    # key: 日期、概念名称、成分股数量、网址、代码
    row_dct = row.to_dict()
    cc_code = row_dct['代码']
    concept = row_dct['概念名称']
    
    stock_board_cons_ths_df = ak.stock_board_cons_ths(symbol=cc_code)
    stock_board_cons_ths_df['概念'] = concept
    stock_board_cons_ths_df = stock_board_cons_ths_df.drop('序号', axis=1)
    df_list.append(stock_board_cons_ths_df)
    time.sleep(5)

ndf = pd.concat(df_list)

def combine(ser):
    if ser.name == '概念':
        return ','.join(ser)
    return ser.tolist()[0]

ndf = ndf.groupby('代码').aggregate(combine)
ndf.to_csv('concept.csv')