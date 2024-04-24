import requests, sys
sys.path += ['/home/ec2-user/tools']
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
    start_date = datetime(2024, 4, 1)
    end_date = datetime(2024, 4, 24)

    dates = mt.get_dates(start_date, end_date)
    s = 'BTCUSDT'
    k = '1m'

    for date in dates:
        #fetch_kline(s, date, kline=k, inst_type='SPOT')
        fetch_kline(s, date, kline=k, inst_type='UFUTURE')


if __name__=="__main__":
    main()
