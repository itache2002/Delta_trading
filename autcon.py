import websocket
import hashlib
import hmac
import base64
import datetime
import json
from delta_rest_client import DeltaRestClient

api_key = 'lbTYnaMuUuDRdgqhRqaw7tcqbK1ReU'
api_secret = 'SNYGrzuvZYFMKeTbvAiHddBrL4PIQMmDockvMHKzIWjtOgjcynaCNXtBXMHl'

def generate_signature(secret, message):
    message = bytes(message, 'utf-8')
    secret = bytes(secret, 'utf-8')
    hash = hmac.new(secret, message, hashlib.sha256)
    return hash.hexdigest()

def get_time_stamp():
    d = datetime.datetime.utcnow()
    epoch = datetime.datetime(1970, 1, 1)
    return str(int((d - epoch).total_seconds()))

def on_message(ws, message):
    data = json.loads(message)
    print(data)

def on_close(ws, close_status_code, close_msg):
    print("Websocket closed")

def on_error(ws, error):
    print(f"Error: {error}")

def on_open(ws):
    print("Websocket opened")

    # Send authentication message
    timestamp = get_time_stamp()
    signature_data = f"GET{timestamp}/live"
    signature = generate_signature(api_secret, signature_data)

    auth_msg = {
        "type": "auth",
        "payload": {
            "api-key": api_key,
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

    

# Create WebSocket instance with on_open, on_message, on_error, and on_close callbacks
ws = websocket.WebSocketApp('wss://socket.delta.exchange', on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)

# Run the WebSocket
ws.run_forever()
