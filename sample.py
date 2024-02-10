import datetime
import requests
import pandas as pd

current_timestamp = int(datetime.datetime.now().timestamp())
last_100_candel = current_timestamp - 90000
print(current_timestamp)
print(last_100_candel)

headers = {
    'Accept': 'application/json'
}

# Sample API request
r = requests.get('https://api.delta.exchange/v2/history/candles', params={
    'resolution': '15m', 'symbol': 'BTCUSDT', 'start':last_100_candel , 'end': current_timestamp
}, headers=headers)

# Check if the request was successful (status code 200)
if r.status_code == 200:
    # Use .json() to extract the JSON data from the response
    json_data = r.json()
    # print(json_data)

    # Assuming the timestamp is under the key 'time'
    df = pd.DataFrame(json_data['result'])
    df['time'] = pd.to_datetime(df['time'], unit='s')  # Update key to match the response

    # Convert the timestamp to 'Asia/Kolkata' timezone (UTC+5:30) and then to string
        
    print(df)

    # Save the DataFrame to an Excel file
    df.to_excel('output_data.xlsx', index=False)


else:
    print(f"Failed to fetch data. Status code: {r.status_code}")
