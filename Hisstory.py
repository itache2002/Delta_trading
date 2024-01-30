import requests
import pandas as pd

headers = {
    'Accept': 'application/json'
}

# Sample API request
r = requests.get('https://api.delta.exchange/v2/history/candles', params={
    'resolution': '15m', 'symbol': 'BTCUSDT', 'start': '1643499730', 'end': '1706635800'
}, headers=headers)

# Check if the request was successful (status code 200)
if r.status_code == 200:
    # Use .json() to extract the JSON data from the response
    json_data = r.json()
    print(json_data)

    # Assuming the timestamp is under the key 'time'
    df = pd.DataFrame(json_data['result'])
    df['time'] = pd.to_datetime(df['time'], unit='s')  # Update key to match the response

    # Convert the timestamp to 'Asia/Kolkata' timezone (UTC+5:30) and then to string
    df['time'] = df['time'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.strftime('%Y-%m-%d %H:%M:%S')

    # Save the DataFrame to an Excel file
    df.to_excel('output_data.xlsx', index=False)

else:
    print(f"Failed to fetch data. Status code: {r.status_code}")
