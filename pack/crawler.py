﻿# -*- coding: utf-8 -*-
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
    
        # 將 date 變成字串 舉例：'20180525' 
        datestr = date.strftime('%Y%m%d')
    
        # 從網站上依照 datestr 將指定日期的股價抓下來
        r = requests.post('http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + datestr + '&type=ALLBUT0999')
    
        # 將抓下來的資料（r.text），其中的等號給刪除
        content = r.text.replace('=', '')
    
        # 將 column 數量小於等於 10 的行數都刪除
        lines = content.split('\n')
        lines = list(filter(lambda l:len(l.split('",')) > 10, lines))
    
        # 將每一行再合成同一行，並用肉眼看不到的換行符號'\n'分開
        content = "\n".join(lines)
    
        # 假如沒下載到，則回傳None（代表抓不到資料）
        if content == '':
            return None
    
        # 將content變成檔案：StringIO，並且用pd.read_csv將表格讀取進來
        df = pd.read_csv(StringIO(content))
    
        # 將表格中的元素都換成字串，並把其中的逗號刪除
        df = df.astype(str)
        df = df.apply(lambda s: s.str.replace(',', ''))
    
        # 將爬取的日期存入 dataframe
        df['date'] = pd.to_datetime(date)
    
        # 將「證券代號」的欄位改名成「stock_id」
        df = df.rename(columns={'證券代號':'stock_id'})
    
        # 將 「stock_id」與「date」設定成index 
        df = df.set_index(['stock_id', 'date'])
    
        # 將所有的表格元素都轉換成數字，error='coerce'的意思是說，假如無法轉成數字，則用 NaN 取代
        df = df.apply(lambda s:pd.to_numeric(s, errors='coerce'))
    
        # 刪除不必要的欄位
        df = df[df.columns[df.isnull().all() == False]]
    
        return df
    
    
    def today_choice(self):
        dft = self.crawl_price()
        close_open = dft['收盤價'] / dft['開盤價']

        dff = dft[(close_open>1.03) & (dft['本益比']<20.0) & (dft['本益比']>0.0) &(dft['成交筆數']>500)]
        return dff[['開盤價','收盤價','本益比']]
    
    
    def write_sql(self):
        conn = sqlite3.connect('stock.sqlite3')
        # 存檔 if_exists='replace' 是說假如sql中已經有 daily_price 這個 dataframe，則取代它
        self.crawl_price().to_sql(('daily_price'+"_"+str(self.y)+"-"+str(self.m)+"-"+str(self.d)), conn, if_exists='replace')
        