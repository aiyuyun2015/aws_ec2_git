import requests, sys
sys.path += ['/home/ec2-user/tools']
import MyTools as mt
import datetime
import requests
import json
import pandas as pd
import numpy as np

def get_timestamp(date):
    # Define the start and end times for the data
    #end_time = datetime.datetime.now()
    #start_time = end_time - datetime.timedelta(days=2)

    # Convert the times to Unix timestamps in milliseconds
    #start_timestamp = int(start_time.timestamp() * 1000)
    #end_timestamp = int(end_time.timestamp() * 1000)
    
    e = pd.Timestamp(f"{date} 8:00:00")
    s = e - datetime.timedelta(hours=24)

    start_timestamp = mt.reverse_datetime_to_int(s)
    end_timestamp = mt.reverse_datetime_to_int(e)


    return start_timestamp, end_timestamp


def get_klines(symbol, start_timestamp, end_timestamp):
   

    # Define the Binance API endpoint for K-line data
    
    endpoint = 'https://api.binance.com/api/v3/klines'

    # Define the parameters for the API request
    # symbol = 'BTCUSDT'
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


def convert_to_df(data):
    # Create a pandas dataframe with the OHLC data and timestamps
    ohlc_data = [[float(kline[1]), float(kline[2]), float(kline[3]), float(kline[4])] for kline in data]
    df = pd.DataFrame(ohlc_data, columns=['Open', 'High', 'Low', 'Close'])
    timestamps = [datetime.datetime.fromtimestamp(int(kline[0]) / 1000) for kline in data]
    df['Timestamp'] = timestamps
    df.set_index('Timestamp', inplace=True)

    return df


def convert_to_df2(data):
    columns = [
    'Open time', 'Open', 'High', 'Low', 'Close', 'Volume',
    'Close time', 'Quote asset volume', 'Number of trades',
    'Taker buy base asset volume', 'Taker buy quote asset volume',
    'Ignore'
    ]

    df = pd.DataFrame(data, columns=columns)


    return df
    


def main(symbol, date='20240420'):
    #s, e = '2023-01-01 00:00:00', '2024-04-22 00:00:00'
    # symbol = 'BTCUSDT'

    s, e = get_timestamp(date) 
    data = get_klines(symbol, s, e)    
    try:
        df = convert_to_df2(data)
    except Exception as e:
        print(data)
        print("[Error] faield to convert to df")
        raise e

    mt.write(df, f'./data/kline/{symbol}/{date}.csv', index=True)

    

if __name__=="__main__":
    params = mt.get_parsers()
    debug = params.debug
    symbol = params.input
    kline = params.kline
    date = str(params.date)


    main(symbol, date)
    


