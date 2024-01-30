import websocket
import json

# Delta Exchange API credentials
api_key = 'HB2ex499EEHRHe4vX9iKvmwnA3vG9H'
symbol = 'BTCUSDT'
bollinger_period = 20
ema_period = 5
order_size = 1  # Contracts to buy/sell
stop_loss_percent = 2  # Stop loss percentage
url = f"wss://socket.delta.exchange?token={api_key}"

def on_message(ws, message):
    data = json.loads(message)
    print(data)

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Websocket closed")

def on_open(ws):
    print("Websocket opened")

    
    subscribe_msg = {
        "type": "subscribe",
        "payload": {
            "channels": [
                {"name": "candlestick_1m", 
                 "symbols": [symbol]},
            ]
        }
    }
    ws.send(json.dumps(subscribe_msg))

if __name__ == "__main__":
    ws = websocket.WebSocketApp(url, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()