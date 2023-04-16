import time
import json
from datetime import datetime
import akshare as ak
import pandas as pd

def ma_calculate(df):
    ma_list = [5, 10, 20, 60, 120]

    for ma_len in ma_list:
        df['ma_' + str(ma_len)] = df['收盘'].rolling(ma_len).mean()

    return df.fillna(0)


def max_close_price_calculate(df):
    # 计算45个交易日的最高收盘价
    df['max_45'] = df['收盘'].rolling(45).max()
    return df


def detail_stock(symbol):
    """
    '日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率'
    :return:
    """
    time.sleep(1)  # 担心封ip
    today = datetime.now().strftime('%Y%m%d')
    # today = '20220827'
    stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date="20220810", end_date=today,
                                            adjust="qfq")
    
    if stock_zh_a_hist_df.empty:
        return 

    df = ma_calculate(stock_zh_a_hist_df)
    df = max_close_price_calculate(df)

    # 偏离10日均线点数
    df['deviate_ma_10'] = (df['收盘'] - df['ma_10']) / df['收盘'] * 100
    # 偏离20日均线点数
    df['deviate_ma_20'] = (df['收盘'] - df['ma_20']) / df['收盘'] * 100
    
    # max price
    df['max_price'] = df[['开盘', '收盘']].max(axis=1)
    df['min_price'] = df[['开盘', '收盘']].min(axis=1)
    
    # 计算昨日收盘价
    df['昨收'] = df['收盘'] - df['涨跌额']

    # 上引线长度
    df['upper_lead'] = (df['最高'] - df['max_price']) / df['昨收'] * 100

    # 实体长度
    df['len_of_entity'] = abs(df['收盘'] - df['开盘']) / df['昨收'] * 100

    # 下引线长度
    df['lower_lead'] = (df['min_price'] - df['最低']) / df['昨收'] * 100

    return df

stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
df = stock_zh_a_spot_em_df[
    stock_zh_a_spot_em_df['代码'].str.startswith('00') | stock_zh_a_spot_em_df['代码'].str.startswith('60')]

code_list = df['代码'].tolist()


def read_data():
    with open('update.json') as fp:
        return json.loads(fp.read())
    
def update_data(data):
    with open('update.json', 'w+') as fp:
        fp.write(json.dumps(data))

#update_data(data)
count = 0
data = read_data()
today_line_str = datetime.now().strftime('%Y%m%d')+'15'

for symbol in code_list:
    now_str = datetime.now().strftime('%Y%m%d%H')
    
    dt_str = data.get(symbol, '')
    # print(today_line_str, now_str)
    if dt_str and dt_str >= today_line_str:
        continue
    
    # symbol = '603366'
    try:
        df = detail_stock(symbol)
        if df is None:
            continue
        df.to_csv('daily_data/'+symbol+'.csv', index=False)
        
    except Exception as e:
        print(symbol)
        print(e)
        time.sleep(5)
        continue
    
    count += 1
    if count % 20 == 0:
        print(count)
        update_data(data)
    
    data[symbol] = now_str

update_data(data)
