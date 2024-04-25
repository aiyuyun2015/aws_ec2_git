import requests, sys, os
MT_PATH = os.environ.get("MT_PATH")
sys.path += [MT_PATH]

import MyTools as mt
import datetime
import requests
import json
import pandas as pd
import numpy as np
import time
from kline import fetch_kline
from datetime import datetime
from itertools import product

def run_spot():
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 4, 24)
    dates = mt.get_dates(start_date, end_date)
    k = '1m'

    spots = mt.get_exg_symbols(exg='Binance', inst_type='SPOT')
    for s, date in product(spots, dates):
        fetch_kline(s, date, kline=k, inst_type='SPOT')

        
def run_ufuture():
    start_date = datetime(2024, 1, 1)
    end_date = datetime(2024, 4, 24)
    dates = mt.get_dates(start_date, end_date)
    k = '1m'

    futures = mt.get_exg_symbols(exg='Binance', inst_type='UFUTURE')
    for s, date in product(futures, dates):
        fetch_kline(s, date, kline=k, inst_type='UFUTURE')

        




def main():
    # 1. get BTC data
    start_date = datetime(2016, 1, 1)
    end_date = datetime(2024, 4, 24)

    dates = mt.get_dates(start_date, end_date)
    s = 'ETHUSDT'
    k = '1m'
    #print(dates)
    #exit(0)
    
    for date in dates:
    
        fetch_kline(s, date, kline=k, inst_type='SPOT')
    
    #for date in dates:
    #    fetch_kline(s, date, kline=k, inst_type='UFUTURE')
        #time.sleep(1)

if __name__=="__main__":
    # main()
    run_ufuture()

