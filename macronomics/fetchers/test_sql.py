# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""


import time

from sqlalchemy import create_engine
from sqlalchemy import Table, Column,  String, Float, Date, MetaData, LargeBinary
from macronomics.fetchers.eurostat_fetcher.eurostat_fetcher import EurostatFetcher

engine = create_engine("postgresql://postgres:@127.0.0.1:5432/test")


a = EurostatFetcher('eurostat')
DATASET = 'namq_10_gdp'


metadata = MetaData()
dataset_table_raw = Table(DATASET + "_raw", metadata,
                      Column('refdate', Date),
                      Column('indicator', String),
                      Column('geo', String),
                      Column('value', Float),
                      Column('attribute', String)
                      )

dataset_table_series = Table(DATASET + "_pd", metadata,
                      Column('name', String,  primary_key=True, nullable=False),
                      Column('pd_series', LargeBinary)
                      )


conn = engine.connect()
#metadata.create_all(engine, tables=[dataset_table_raw, dataset_table_series])
#
#start = time.time()
#xml_dsd, xml_data = a._dl_dataset(DATASET)
#end = time.time();
#print("time to download and parse dataset: {}".format(end-start))
#
#
#start = time.time()
#a._bulk_store(xml_data, xml_dsd, conn, dataset_table_raw)
#end = time.time();
#print("time to store data as raw: {}".format(end-start))
#
#
#start = time.time()
#ss = [s for s in a._series(xml_dsd=xml_dsd,xml_data=xml_data)]
#end = time.time(); 
#print("time to create {} pandas series: {}".format(len(ss), end-start))

start = time.time()
a._bulk_store_series(ss, conn, dataset_table_series)
end = time.time(); 
print("time to store data as binary blobs: {}".format(end-start))


