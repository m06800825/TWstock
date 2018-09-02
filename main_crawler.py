# -*- coding: utf-8 -*-
"""
Created on Sat Sep  1 17:36:26 2018

@author: Kempinski
"""

import datetime
from pack import crawler

x = datetime.date.today() - datetime.timedelta(days=2)
year = x.year
month = x.month
day = x.day

a = crawler.Crawl(year, month, day)
a.write_sql_dailyprice()
print(a.today_choice())

b = crawler.Crawl(2018, 3, 1)
print(b.crawl_monthly_report())