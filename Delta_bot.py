import requests
import pandas as pd
import json
import datetime
import hmac
import hashlib
import numpy as np
from talib import abstract
import websocket

class Delta:
    def __init__(self, api_key, api_secret, leverage):
        self.api_key = api_key
        self.api_secret = api_secret
        self.leverage = leverage
        self.headers = {'Accept': 'application/json'}
        self.history_df = None

    def fetch_history(self):
        current_timestamp = int(datetime.datetime.now().timestamp())
        last_100_candle = current_timestamp - 90000
        params = {
            'resolution': '15m',
            'symbol': 'BTCUSDT',
            'start': last_100_candle,
            'end': current_timestamp
        }
        try:
            response = requests.get('https://api.delta.exchange/v2/history/candles', params=params, headers=self.headers)
            response.raise_for_status()  # Raises a HTTPError if the status is 4xx, 5xx
            data = response.json()
            df = pd.DataFrame(data['result'])
            df['time'] = pd.to_datetime(df['time'], unit='s')
            df['time'] = df['time'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.strftime('%Y-%m-%d %H:%M:%S')
            self.history_df = df
            self._calculate_indicators()
            print(self.history_df)
        except requests.RequestException as e:
            print(f"Failed to fetch historical data: {e}")

    def _calculate_indicators(self):
        self.history_df['EMA'] = abstract.EMA(self.history_df['close'], timeperiod=5)
        self.history_df['upper'], self.history_df['middleband'], self.history_df['lower'] = abstract.BBANDS(self.history_df['close'], timeperiod=15, nbdevup=3.0, nbdevdn=3.0, matype=0)


    def on_message(self, ws, message):
        data = json.loads(message)
        print(data)

    def on_error(self, ws, error):
        print(f"WebSocket error: {error}")

    def on_close(self, ws):
        print("WebSocket closed")

    def on_open(self, ws):
        print("WebSocket opened. Subscribing...")
        auth_message = self._generate_auth_message()
        ws.send(json.dumps(auth_message))
        subscribe_message = {
            "type": "subscribe",
            "payload": {
                "channels": [{"name": "candlestick_1m", "symbols": ['BTCUSDT']}]
            }
        }
        ws.send(json.dumps(subscribe_message))

    def _generate_auth_message(self):
        timestamp = str(int(datetime.datetime.utcnow().timestamp()))
        message = f"GET{timestamp}/live"
        signature = hmac.new(self.api_secret.encode(), message.encode(), hashlib.sha256).hexdigest()
        return {
            "type": "auth",
            "payload": {
                "api-key": self.api_key,
                "signature": signature,
                "timestamp": timestamp
            }
        }

    def live_data(self):
        ws_url = 'wss://socket.delta.exchange'
        ws = websocket.WebSocketApp(ws_url, on_message=self.on_message, on_error=self.on_error, on_close=self.on_close, on_open=self.on_open)
        ws.run_forever()


# Initialize and use your Delta instance
api_key = 'VraFQTfHWk1w7Id1uC1pJvyKQ5AWy5'
api_secret = 'XJofp39iR4p7092qkzJdH9VPmoZmHtWv2x0huuJWscPerHtGqcf2Uo996JMj'
leverage = 25
delta = Delta(api_key, api_secret, leverage)

delta.fetch_history()
delta.live_data()  # Uncomment to start receiving live data
