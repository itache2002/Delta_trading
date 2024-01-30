import requests
import pandas as pd
import websocket
import datetime
import json
from delta_rest_client import DeltaRestClient, create_order_format, round_by_tick_size, cancel_order_format, OrderType
from talib import abstract
import hashlib
import hmac
import base64

class Delat():
    def __init__(self,api_key, api_secret,lev):
        self.api_key = api_key
        self.api_secret = api_secret
        self.leverage = lev
        self.EMA = 5
        self.BBL = 15
        self.BBSD = 3
        self.todate = None
        self.yesterday = None
        self.headers = {'Accept': 'application/json'}
        self.historydf = None
    def getDate(self):
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        # Convert date objects to datetime objects
        today_datetime = datetime.datetime.combine(today, datetime.datetime.min.time())
        yesterday_datetime = datetime.datetime.combine(yesterday, datetime.datetime.min.time())

        today_timestamp = datetime.datetime.timestamp(today_datetime)
        yesterday_timestamp = datetime.datetime.timestamp(yesterday_datetime)
        self.todate = int(today_timestamp)
        self.yesterday = int(yesterday_timestamp)

    def History(self):
        self.getDate()
        params = {
            'resolution': '15m',
            'symbol': 'BTCUSDT',
            'start':self.todate,  # Convert to string if not already
            'end': self.yesterday  # Convert to string if not already
        }

        r = requests.get('https://api.delta.exchange/v2/history/candles', params=params, headers=self.headers)
        if r.status_code == 200:
            # Use .json() to extract the JSON data from the response
            json_data = r.json()
            print(json_data)
            # Assuming the timestamp is under the key 'time'
            df = pd.DataFrame(json_data['result'])
            print(df.columns)
            df['time'] = pd.to_datetime(df['time'], unit='s')  # Update key to match the response

            # Convert the timestamp to 'Asia/Kolkata' timezone (UTC+5:30) and then to string
            df['time'] = df['time'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.strftime('%Y-%m-%d %H:%M:%S')
            
            df.to_excel('output_data.xlsx', index=False)
            # # Save the DataFrame to an Excel file
            

        else:
            print(f"Failed to fetch data. Status code: {r.status_code}")


    def generate_signature(self,secret, message):
        message = bytes(message, 'utf-8')
        secret = bytes(secret, 'utf-8')
        hash = hmac.new(secret, message, hashlib.sha256)
        return hash.hexdigest()

    def get_time_stamp(self):
        d = datetime.datetime.utcnow()
        epoch = datetime.datetime(1970, 1, 1)
        return str(int((d - epoch).total_seconds()))
    
    def on_message(self,ws, message):
        data = json.loads(message)
        print(data)

    def on_close(self,ws, close_status_code, close_msg):
        print("Websocket closed")

    def on_error(self,ws, error):
        print(f"Error: {error}")

    def on_open(self,ws):
        print("Websocket opened")
    # Send authentication message
        timestamp = self.get_time_stamp()
        signature_data = f"GET{timestamp}/live"
        signature = self.generate_signature( self.api_secret, signature_data)

        auth_msg = {
            "type": "auth",
            "payload": {
                "api-key": self.api_key,
                "signature": signature,
                "timestamp": timestamp
            }
        }
        ws.send(json.dumps(auth_msg))

        # Subscribe to channels
        subscribe_msg = {
            "type": "subscribe",
            "payload": {
                "channels": [
                    {"name": "candlestick_1m", "symbols": ['BTCUSDT']},
                ]
            }
        }
        ws.send(json.dumps(subscribe_msg))

    def live_data(self):
        ws = websocket.WebSocketApp('wss://socket.delta.exchange', on_open=self.on_open, on_message=self.on_message, on_error=self.on_error, on_close=self.on_close)
        # Run the WebSocket
        ws.run_forever()


api_key='lbTYnaMuUuDRdgqhRqaw7tcqbK1ReU',
api_secret='SNYGrzuvZYFMKeTbvAiHddBrL4PIQMmDockvMHKzIWjtOgjcynaCNXtBXMHl'

delta = Delat(api_key, api_secret , 25)

delta.History()
# delta.live_data()