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


def get_klines(symbol, start_timestamp, end_timestamp, kline=None, endpoint=None):
   

    # Define the Binance API endpoint for K-line data
    #if not endpoint:    
    #    endpoint = 'https://api.binance.com/api/v3/klines'
    print("Use endpoing:==>", endpoint)
    
    # Define the parameters for the API request
    if not kline:
        kline = '15m'
    limit = 1000
    params = {'symbol': symbol, 'interval': kline, 'startTime': start_timestamp, 'endTime': end_timestamp, 'limit': limit}

    # Send the API request and store the response data in a list
    data = []
    while True:
        response = requests.get(endpoint, params=params)
        used_weight_header = response.headers.get('X-MBX-USED-WEIGHT-1m')
        if used_weight_header is not None:
            # Parse the used weight value
            used_weight = int(used_weight_header)
            print("Used weight:", used_weight)
        else:
            print("X-MBX-USED-WEIGHT-1m header not found in response.")



        klines = json.loads(response.text)
        data += klines
        if len(klines) < limit:
            break
        params['startTime'] = int(klines[-1][0]) + 1
        if used_weight > 300:
            time.sleep(15 * 60)
        else:
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
    

def fetch_kline(symbol, date=None, kline=None, inst_type=None, exchg='Binance', debug=False):
    
    ofile = f'./data/{kline}/{exchg}/{symbol}-{inst_type}/{date}.csv'
    if os.path.exists(ofile) and not debug:
        print(f'[Warning] file {ofile} exists, skip..')
        return 

    s, e = get_timestamp(date) 

    url = mt.get_basepoint(exchg, inst_type) + 'klines'
    data = get_klines(symbol, s, e, kline, url)    
    try:
        df = convert_to_df2(data)
    except Exception as e:
        print(data)
        print("[Error] faield to convert to df")
        raise e

    mt.write(df, ofile, index=True)

    
if __name__=="__main__":
    params = mt.get_parsers()
    debug = params.debug
    symbol = params.input
    kline = params.kline
    date = str(params.date)
    inst_type = params.inst_type

    if debug:
        symbol = 'BTCUSDT'
        kline = '1m'
        date = '20240402'
        inst_type = 'UFUTURE'

    fetch_kline(symbol, date, kline, inst_type, exchg='Binance', debug=debug)
    
