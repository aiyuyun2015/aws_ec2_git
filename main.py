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


def main():
    # 1. get BTC data
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 4, 24)

    dates = mt.get_dates(start_date, end_date)
    s = 'BTCUSDT'
    k = '1m'
    #print(dates)
    #exit(0)
    
    for date in dates:
    
        fetch_kline(s, date, kline=k, inst_type='SPOT')
    
    for date in dates:
        fetch_kline(s, date, kline=k, inst_type='UFUTURE')
        #time.sleep(1)

if __name__=="__main__":
    main()
