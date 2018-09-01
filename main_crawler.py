# -*- coding: utf-8 -*-
"""
Created on Sat Sep  1 17:36:26 2018

@author: Kempinski
"""

import datetime
from pack import crawler

x = datetime.date.today() - datetime.timedelta(days=1)
year = x.year
month = x.month
day = x.day

a = crawler.Crawl(year, month, day)
a.write_sql()
print(a.today_choice())