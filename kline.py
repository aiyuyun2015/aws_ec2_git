import requests, sys
sys.path += ['/home/ec2-user/tools']
import MyTools as mt
import datetime
import requests
import json
import pandas as pd
import numpy as np
import time


def get_timestamp(date):
    """
    16:00(T-1) --> 16:00 (T) in our file.
    
    UTC-0, instead of UTC-8.

    """
    e = pd.Timestamp(f"{date} 8:00:00")
    s = e - datetime.timedelta(hours=24)

    start_timestamp = mt.reverse_datetime_to_int(s)
    end_timestamp = mt.reverse_datetime_to_int(e)

    return start_timestamp, end_timestamp


def get_klines(symbol, start_timestamp, end_timestamp, interval=None, endpoint=None):
   

    # Define the Binance API endpoint for K-line data
    if not endpoint:    
        endpoint = 'https://api.binance.com/api/v3/klines'

    # Define the parameters for the API request
    if not interval:
        interval = '15m'
    limit = 1000
    params = {'symbol': symbol, 'interval': interval, 'startTime': start_timestamp, 'endTime': end_timestamp, 'limit': limit}

    # Send the API request and store the response data in a list
    data = []
    while True:
        response = requests.get(endpoint, params=params)
        klines = json.loads(response.text)
        data += klines
        if len(klines) < limit:
            break
        params['startTime'] = int(klines[-1][0]) + 1
        time.sleep(0.1)

    return data


def convert_to_df2(data):
    columns = [
    'Open time', 'Open', 'High', 'Low', 'Close', 'Volume',
    'Close time', 'Quote asset volume', 'Number of trades',
    'Taker buy base asset volume', 'Taker buy quote asset volume',
    'Ignore'
    ]

    df = pd.DataFrame(data, columns=columns)

    return df
    

def fetch_kline(symbol, date='20240420', kline=None, inst_type='SPOT', exchg='Binance'):

    s, e = get_timestamp(date) 

    url = mt.get_basepoint(exchg, inst_type) + '/klines'
    data = get_klines(symbol, s, e, kline)    
    try:
        df = convert_to_df2(data)
    except Exception as e:
        print(data)
        print("[Error] faield to convert to df")
        raise e

    mt.write(df, f'./data/{kline}/{exchg}/{symbol}-{inst_type}/{date}.csv', index=True)

    
if __name__=="__main__":
    params = mt.get_parsers()
    debug = params.debug
    symbol = params.input
    kline = params.kline
    date = str(params.date)
    inst_type = params.inst_type

    if debug:
        symbol = 'BTCUSDT'
        kline = '15m'
        date = '20240402'
        inst_type = 'SPOT'

    fetch_kline(symbol, date, kline, inst_type)
    
