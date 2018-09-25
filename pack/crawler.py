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
import time
import os

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
        
        conn = sqlite3.connect('stock.sqlite3')
        df.to_sql(('daily_price'+"_"+str(self.y)+"-"+str(self.m)+"-"+str(self.d)), conn, if_exists='replace')
        
        return df
    
    
    
    def today_choice(self):
        dft = self.crawl_price()
        close_open = dft['收盤價'] / dft['開盤價']

        dff = dft[(close_open>1.03) & (dft['本益比']<20.0) & (dft['本益比']>0.0) &(dft['成交筆數']>500)]
        
        return dff[['開盤價','收盤價','本益比']]
    
        
    
    def crawl_monthly_report(self):
        date = datetime.date(self.y,self.m,self.d)
        url = 'http://mops.twse.com.tw/nas/t21/sii/t21sc03_'+str(date.year - 1911)+'_'+str(date.month)+'_0.html'
        if date.year - 1991 <= 98: #民國98年以前的月報爬取網址不一樣
            url = 'http://mops.twse.com.tw/nas/t21/sii/t21sc03_'+str(date.year - 1911)+'_'+str(date.month)+'.html'
    
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
 
        r = requests.get(url, headers)
        r.encoding = 'big5'
        
        try:
            html_df = pd.read_html(StringIO(r.text))
        except:
            return None

        if len(html_df[0]) > 500:
            df = html_df[0].copy()
        else:
            df = pd.concat([df for df in html_df if df.shape[1] <= 11])

        df = df[list(range(0,10))]
        column_index = df.index[(df[0] == '公司代號')][0]
        df.columns = df.iloc[column_index]
        df['當月營收'] = pd.to_numeric(df['當月營收'], errors='coerce')
        df = df[~df['當月營收'].isnull()]
        df = df[df['公司代號'] != '合計']
        next_month = datetime.date(date.year + int(date.month / 12), ((date.month % 12) + 1), 10)
        df['date'] = pd.to_datetime(next_month)
        df = df.rename(columns={'公司代號':'stock_id'})
        df = df.set_index(['stock_id', 'date'])
        df = df.apply(lambda s:pd.to_numeric(s, errors='coerce'))
        df = df[df.columns[df.isnull().all() == False]]
        
        conn = sqlite3.connect('stock.sqlite3')
        df.to_sql('monthly_report'+"_"+str(self.y)+"-"+str(self.m), conn, if_exists='replace')
        
        return df
    
    
    def save_season_report(self, stock_id):    
        if 'season_report' not in os.listdir():
            os.mkdir('season_report')
    
        # 爬取html檔
        res = requests.get('http://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID=' + stock_id + '&SYEAR=2017&SSEASON=3&REPORT_ID=C')
        res.encoding = 'big5'
    
        # 設定存檔路徑
        path = os.path.join('season_report', stock_id + '.html')
    
        # 檔案打開，寫入html，然後關閉
        f = open(path, 'w', encoding='utf-8')
        f.write(res.text)
        f.close()
    
        print(stock_id)
    
        # 休息10秒
        time.sleep(10)