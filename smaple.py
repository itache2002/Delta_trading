import datetime

# Get today's date
today = datetime.date.today()

# Get yesterday's date
yesterday = today - datetime.timedelta(days=1)

# Convert date objects to datetime objects
today_datetime = datetime.datetime.combine(today, datetime.datetime.min.time())
yesterday_datetime = datetime.datetime.combine(yesterday, datetime.datetime.min.time())

# Get timestamps
today_timestamp = datetime.datetime.timestamp(today_datetime)
yesterday_timestamp = datetime.datetime.timestamp(yesterday_datetime)

# Convert timestamps to integers
today_timestamp_int = int(today_timestamp)
yesterday_timestamp_int = int(yesterday_timestamp)

# Print the results
print("Today's date:", today)
print("Yesterday's date:", yesterday)
print("Today's timestamp:", today_timestamp_int)
print("Yesterday's timestamp:", yesterday_timestamp_int)
