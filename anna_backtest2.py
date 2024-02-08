import requests
import pandas as pd
from talib import abstract
from tabulate import tabulate

class BackTest:
    def __init__(self):
        self.columns = ['Equity Name', 'Trade', 'Entry Time', 'Entry Price', 'Exit Time', 'Exit Price', 'Quantity', 'Position Size', 'PNL', '% PNL']
        self.backtesting = []  # Initialize backtesting as an empty list
        self.trade_log = {}
        self.total_pnl = 0
        self.paused = False
        self.stop_loss_percentage = 0.02


    def buy(self, equity_name, entry_time, entry_price, qty):
        self.trade_log = dict(zip(self.columns, [None] * len(self.columns)))
        self.trade_log['Trade'] = 'Trade Open'
        self.trade_log['Quantity'] = qty
        self.trade_log['Position Size'] = round(self.trade_log['Quantity'] * entry_price, 3)
        self.trade_log['Equity Name'] = equity_name
        self.trade_log['Entry Time'] = entry_time
        self.trade_log['Entry Price'] = round(entry_price, 2)
        self.paused = False
        self.stop_loss_price = entry_price * (1 - self.stop_loss_percentage)
        

    def sell(self, exit_time, exit_price, exit_type, charge):
        if 'Entry Price' not in self.trade_log:
            # Entry information is missing, skip the sell operation
            return

        self.trade_log['Trade'] = 'Trade Closed'
        self.trade_log['Exit Time'] = exit_time
        self.trade_log['Exit Price'] = round(exit_price, 2)
        self.trade_log['Exit Type'] = exit_type

        # Adjust stop-loss to exit at the lower of stop-loss and exit_price
        self.stop_loss_price = min(self.stop_loss_price, exit_price)

        # Check if the stop-loss is triggered
        if exit_price < self.stop_loss_price:
            self.trade_log['Exit Type'] = 'Stop Loss Triggered'

        self.trade_log['PNL'] = round((exit_price - self.trade_log['Entry Price']) * self.trade_log['Quantity'] - charge, 3)
        self.trade_log['% PNL'] = round((self.trade_log['PNL'] / self.trade_log['Position Size']) * 100, 3)
        self.total_pnl += self.trade_log['PNL']

        # Check if profit or loss has reached 200 points
        if abs(self.total_pnl) >= 0:
            self.trade_log['Exit Type'] = 'Target Achieved'
            self.backtesting.append(self.trade_log)
            self.reset()

    def reset(self):
        self.total_pnl = 0
        self.paused = False

    def pause(self):
        self.paused = True

    def resume(self):
        self.paused = False

    def stats(self):
        df = pd.DataFrame(self.backtesting)
        parameters = ['Total Trade Scripts', 'Total Trade', 'PNL', 'Winners', 'Losers', 'Win Ratio', 'Total Profit', 'Total Loss', 'Average Loss per Trade', 'Average Profit per Trade', 'Average PNL Per Trade', 'Risk Reward']
        total_traded_scripts = len(df['Equity Name'].unique())
        total_trade = len(df.index)
        pnl = df.PNL.sum()
        winners = len(df[df.PNL > 0])
        losers = len(df[df.PNL <= 0])
        win_ratio = str(round((winners / total_trade) * 100, 2)) + '%'
        total_profit = round(df[df.PNL > 0].PNL.sum(), 2)
        total_loss = round(df[df.PNL <= 0].PNL.sum(), 2)
        average_loss_per_trade = round(total_loss / losers, 2)
        average_profit_per_trade = round(total_profit / winners, 2)
        average_pnl_per_trade = round(pnl / total_trade, 2)
        risk_reward = f'1:{-1 * round(average_profit_per_trade / average_loss_per_trade, 2)}'
        data_points = [total_traded_scripts, total_trade, pnl, winners, losers, win_ratio, total_profit, total_loss, average_loss_per_trade, average_profit_per_trade, average_pnl_per_trade, risk_reward]
        data = list(zip(parameters, data_points))
        print(tabulate(data, ['Parameters', 'Values'], tablefmt='psql'))


# Sample API request
headers = {'Accept': 'application/json'}
r = requests.get('https://api.delta.exchange/v2/history/candles', params={
    'resolution': '15m', 'symbol': 'BTCUSDT', 'start': '1698802110', 'end': '1703986110'
}, headers=headers)

if r.status_code == 200:
    json_data = r.json()
    df = pd.DataFrame(json_data['result'])
    df['time'] = pd.to_datetime(df['time'], unit='s')  
    df['time'] = df['time'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.strftime('%Y-%m-%d %H:%M:%S')
    df.to_excel('output_data.xlsx', index=False)

    df['upper'], df['middleband'], df['lower'] = abstract.BBANDS(df['close'], timeperiod=15, nbdevup=3.0, nbdevdn=3.0, matype=0)
    df['EMA'] = abstract.EMA(df.close, timeperiod=5)
    df['crossup'] = df['crossdown'] = 0

    for i in range(15, len(df)):
        if df['middleband'][i - 1] <= df['EMA'][i - 1] and df['middleband'][i] > df['EMA'][i]:
            df.at[i, 'crossup'] = 1
        elif df['middleband'][i - 1] >= df['EMA'][i - 1] and df['middleband'][i] < df['EMA'][i]:
            df.at[i, 'crossdown'] = 1

    filtered_data = df[(df['crossup'] == 1) | (df['crossdown'] == 1)]

    bt = BackTest()
    capital = 500000

    for i in range(1, len(filtered_data)):
        data = filtered_data.iloc[i]

        if not bt.paused:
            if data['crossup'] == 1:
                qty = capital // data['open']
                bt.buy('BTCUSDT', data['time'], data['open'], qty)
            elif data['crossdown'] == 1:
                bt.sell(data['time'], data['open'], 'Exit', 0)

            # Check for Bollinger Bands and EMA crossover in two consecutive candles
            if (
                i + 1 < len(filtered_data)
                and data['crossup'] == 1
                and filtered_data.iloc[i + 1]['crossup'] == 1
            ):
                bt.pause()

        elif abs(data['upper'] - data['EMA']) >= 100:
            bt.resume()

    # Print statistics after all trades are executed
    bt.stats()
else:
    print(f"Failed to fetch data. Status code: {r.status_code}")

df = pd.DataFrame(bt.backtesting)

df.to_excel('output_anna2.xlsx', index=False)