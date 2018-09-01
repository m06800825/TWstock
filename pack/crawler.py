# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import requests
import datetime
import pandas as pd
from io import StringIO
import sqlite3


class Crawl():
    def __init__(self, year, month, day):
        self.y = year
        self.m = month
        self.d = day
        
        
    def crawl_price(self):
        date = datetime.datetime(self.y, self.m, self.d)
        datestr = date.strftime('%Y%m%d')
    
        r = requests.post('http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + datestr + '&type=ALLBUT0999')

        content = r.text.replace('=', '')
        lines = content.split('\n')
        lines = list(filter(lambda l:len(l.split('",')) > 10, lines))
        content = "\n".join(lines)
    
        if content == '':
            return None
    
        df = pd.read_csv(StringIO(content))
        df = df.astype(str)
        df = df.apply(lambda s: s.str.replace(',', ''))
        df['date'] = pd.to_datetime(date)
        df = df.rename(columns={'證券代號':'stock_id'})
        df = df.set_index(['stock_id', 'date'])
        df = df.apply(lambda s:pd.to_numeric(s, errors='coerce'))
        df = df[df.columns[df.isnull().all() == False]]
    
        return df
    
    
    def today_choice(self):
        dft = self.crawl_price()
        close_open = dft['收盤價'] / dft['開盤價']

        dff = dft[(close_open>1.03) & (dft['本益比']<20.0) & (dft['本益比']>0.0) &(dft['成交筆數']>500)]
        return dff[['開盤價','收盤價','本益比']]
    
    
    def write_sql(self):
        conn = sqlite3.connect('stock.sqlite3')
        self.crawl_price().to_sql(('daily_price'+"_"+str(self.y)+"-"+str(self.m)+"-"+str(self.d)), conn, if_exists='replace')
        