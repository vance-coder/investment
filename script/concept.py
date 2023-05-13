import time
import akshare as ak
import json
import pymongo
import pandas as pd
from datetime import datetime

data = {}
df_list = []

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["dbs"]
concept_col = mydb.concept
today_line_str = datetime.now().strftime('%Y%m%d')

stock_board_concept_name_ths_df = ak.stock_board_concept_name_ths()

for idx, row in stock_board_concept_name_ths_df.iterrows():
    # key: 日期、概念名称、成分股数量、网址、代码
    row_dct = row.to_dict()
    cc_code = row_dct['代码']
    concept = row_dct['概念名称']
    
    res = concept_col.find_one({'_id': concept})
    if res:
        if res['time'].split()[0].replace('-', '') == today_line_str:
            continue
    
    now_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    try:
        stock_board_cons_ths_df = ak.stock_board_cons_ths(symbol=cc_code)
        stock_board_cons_ths_df['概念'] = concept
        stock_board_cons_ths_df = stock_board_cons_ths_df.drop('序号', axis=1)
        dct = stock_board_cons_ths_df.to_dict(orient='records')
    except:
        print(concept)
        time.sleep(10)
        continue

    data = {
        '_id':concept,
        'time': now_datetime,
        'data': dct
    }

    concept_col.update_one({'_id': concept}, {'$set': data}, upsert=True)
    
    time.sleep(3)
