# save last day history
import os
import time
import json
from datetime import datetime
import pandas as pd
import akshare as ak

import ssl
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart

def get_text():

    data = []
    folder = 'daily_data/'

    concept = pd.read_csv('concept.csv')

    cdf = ak.stock_zh_a_spot_em()
    # 只要主板的票
    cdf = cdf[cdf['代码'].str.startswith('00') | cdf['代码'].str.startswith('60')]
    # 过滤st
    cdf['名称'] = cdf['名称'].str.lower()
    cdf = cdf[~cdf['名称'].str.contains('st')]
    # 开盘涨幅
    cdf['open_range'] = (df['今开'] - df['昨收'])/df['昨收'] * 100
    cdf['open_range'] = cdf['open_range'].round(2)
    # 开盘涨幅filter
    cdf = cdf[(1.8 <= cdf['open_range']) & (cdf['open_range'] <= 4)]

    for idx, ser in cdf.iterrows():
        row = {}
        dct = ser.to_dict()
        code = dct['代码']
        ndf = pd.read_csv(os.path.join(folder, str(code)+'.csv'))
        last_dct = ndf.iloc[-2].to_dict()

        concept_df = concept[concept['代码'].isin([code])]
        if concept_df.empty:
            continue
        concept_dct = concept_df.iloc[0].to_dict()

        # 次新股不考虑
        if '次新' in concept_dct['概念']:
            continue

        if 'st' in dct['名称'].lower():
            continue

        # 趋势是好的，10日线大于20日线
        if last_dct['ma_10'] < last_dct['ma_20']*0.98:
            continue

        # 破二十日线也不考虑
        if last_dct['昨收'] < last_dct['ma_20']*0.98:
            continue

        # 流通市值太小的不考虑
        if float(concept_dct["流通市值"][:-1]) < 20:
            continue

        # 昨天收盘需要小于-3.45
        if not (last_dct['涨跌幅'] <= -3.45):
            continue

        row['代码'] = code
        # row['日期'] = last_dct['日期']
        row['名称'] = dct['名称']
        row['昨收'] = last_dct['涨跌幅']
        row['开盘'] = dct['open_range']
        row['流通市值'] = concept_dct["流通市值"]
        row['概念'] = concept_dct['概念']

        data.append(row)

    # pd.DataFrame(data)
    text = f'{datetime.now()}\n'
    for row in data:
        text += '-'*25 + '\n'
        for k, v in row.items():
            text += f'{k} : {v}\n'
    return text

def send_mail(text):
    key = 'letadehhnxtpbdca'
    EMAIL_ADDRESS = 'wikizeros@qq.com'
    EMAIL_PASSWORD = key
    context = ssl.create_default_context()
    sender = EMAIL_ADDRESS
    receiver = ['vance.coder@139.com']

    subject = datetime.now().strftime('%Y%m%d%H%M%S')
    msg = EmailMessage()
    msg['subject'] = subject
    msg['From'] = sender
    msg['To'] = receiver
    msg.set_content(text)

    # msg.attach(MIMEText(html_msg, "html", "utf-8"))

    with smtplib.SMTP_SSL("smtp.qq.com", 465, context=context) as smtp:
        # smtp.set_debuglevel(1)
        smtp.ehlo('smtp.qq.com')
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        smtp.send_message(msg)

text = get_text()
send_mail(text)
