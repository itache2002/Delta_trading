import pandas as pd
from functools import lru_cache
import datetime
import requests
import time

def date_to_timestamp(datetime_object):
    return int(time.mktime(datetime_object.timetuple()))

@lru_cache(maxsize=None)  # Unbounded cache
def query_api(start_timestamp, end_timestamp, resolution, symbol):
    url = f'https://p-api.delta.exchange/v2/chart/history?symbol=MARK:{symbol}&resolution={resolution}&from={start_timestamp}&to={end_timestamp}&cache_ttl=1d'
    # print(url)
    response = requests.get(url)
    response_data = response.json()
    return response_data



@lru_cache(maxsize=None)  # Unbounded cache
def get_btc_ohlc_recursive(start_timestamp, end_timestamp, resolution, symbol):
    response_data = query_api(start_timestamp, end_timestamp, resolution, symbol)

    if response_data['success']:
        df = pd.DataFrame(response_data['result'])
        if df.empty:
            return df  # Base case: if DataFrame is empty, return it

        df['time'] = pd.to_datetime(df['t'], unit='s')
        # Check if there's a gap in the data
        last_timestamp = df['t'].iloc[-1]
        if last_timestamp < end_timestamp:
            next_start_timestamp = last_timestamp + 1
            # Recursive call for the missing timeframe
            next_df = get_btc_ohlc_recursive(next_start_timestamp, end_timestamp, resolution, symbol)
            df = pd.concat([df, next_df], ignore_index=True)
    else:
        print('error')
        df = pd.DataFrame()

    return df

@lru_cache(maxsize=None)  # Unbounded cache
def get_btc_ohlc(start_date, end_date, resolution, symbol='BTCUSDT'):

    # timestamps are UTC
    start_timestamp = date_to_timestamp(start_date)
    end_timestamp = date_to_timestamp(end_date)
    return get_btc_ohlc_recursive(start_timestamp, end_timestamp, resolution, symbol)

if __name__ == '__main__':
    start_date = datetime.datetime(2024,2,7)
    end_date = datetime.datetime(2024,2,11)
    resolution = '15'
    symbol = 'BTCUSDT'
    df = get_btc_ohlc(start_date, end_date, resolution, symbol)
    df.to_excel("last100candels.xlsx")
