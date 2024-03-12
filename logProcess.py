import pandas as pd
import re

# Load the log data into a Pandas DataFrame
logs = pd.read_csv('server_logs.csv')

# Drop duplicate rows
logs.drop_duplicates(inplace=True)

# Handle missing values
logs = logs.dropna(subset=['timestamp', 'log_message'])

# Extract relevant columns
logs = logs[['timestamp', 'log_level', 'log_message']]

# Format timestamp column
logs['timestamp'] = pd.to_datetime(logs['timestamp'], format='%Y-%m-%d %H:%M:%S')

# Define a regular expression pattern to parse structured log messages
log_pattern = r'(?P<client_ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - (?P<user>[\w-]+) \[(?P<timestamp>\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})\] "(?P<request_method>\w+) (?P<request_path>\S+) (?P<request_protocol>\S+)" (?P<response_code>\d+) (?P<response_size>\d+)'

# Parse the log messages using the regular expression
log_data = logs['log_message'].str.extract(log_pattern, re.IGNORECASE)

# Concatenate the parsed log data to the original DataFrame
logs = pd.concat([logs, log_data], axis=1)

# Save the cleaned data to a new CSV file
logs.to_csv('cleaned_logs.csv', index=False)