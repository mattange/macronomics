# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import time

from arctic import Arctic
from macronomics.fetchers.eurostat_fetcher.eurostat_fetcher import EurostatFetcher
store = Arctic('127.0.0.1:27017')
library = store['mattange.test1']

a = EurostatFetcher('eurostat')
DATASET = 'namq_10_gdp'


start = time.time()
xml_dsd, xml_data = a._dl_dataset(DATASET)
end = time.time(); 
print("time to download and parse dataset: {}".format(end-start))


start = time.time()
ss = [s for s in a._series(xml_dsd=xml_dsd,xml_data=xml_data)]
end = time.time(); 
print("time to create {} pandas series: {}".format(len(ss), end-start))


start = time.time()
for s in ss:
    library.write(s.name, s)
end = time.time();
print("time to store {} pandas series: {}".format(len(ss), end-start))