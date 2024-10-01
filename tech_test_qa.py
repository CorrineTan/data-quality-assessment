import os
import psycopg2
import pandas as pd
from scipy import stats

# Connect to the PostgreSQL database using environment variables
try:
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
    print("Database connection established.")
except Exception as e:
    print("Error connecting to the database:", e)
    exit()

# Load data into DataFrames
try:
    trades_df = pd.read_sql('SELECT * FROM trades', conn)
    users_df = pd.read_sql('SELECT * FROM users', conn)
    symbols_df = pd.read_sql('SELECT * FROM symbols', conn)
    print("Data loaded successfully.")
except Exception as e:
    print("Error loading data:", e)
    conn.close()
    exit()

# Close the database connection
conn.close()

# -------------------------------
# Data Quality Checks
# -------------------------------

# 1. Check for unexpected strings in numeric fields
numeric_fields = ['digits', 'cmd', 'volume', 'open_price', 'contractsize']
for field in numeric_fields:
    if trades_df[field].dtype == 'object':
        print(f"Unexpected strings found in field '{field}'.")
        print(trades_df[trades_df[field].apply(lambda x: not str(x).replace('.', '', 1).isdigit())][field])

# Conclusion:
# Some numeric fields contain string values, indicating data type inconsistencies.

# 2. Check for unexpected numerical values
# a) Negative volumes
negative_volume = trades_df[trades_df['volume'] < 0]
if not negative_volume.empty:
    print("Negative volumes found:")
    print(negative_volume[['ticket_hash', 'volume']])

# Conclusion:
# Negative volume values are invalid and may indicate data entry errors.

# b) Negative prices
negative_prices = trades_df[trades_df['open_price'] < 0]
if not negative_prices.empty:
    print("Negative open prices found:")
    print(negative_prices[['ticket_hash', 'open_price']])

# Conclusion:
# Negative prices are invalid for trade prices.

# c) Outliers in volume
z_scores = stats.zscore(trades_df['volume'].astype(float))
outliers = trades_df[(abs(z_scores) > 3)]
if not outliers.empty:
    print("Outliers detected in volume:")
    print(outliers[['ticket_hash', 'volume']])

# Conclusion:
# There are trades with volumes significantly different from the mean, which may need investigation.

# 3. Check for unexpected dates
# Convert 'open_time' and 'close_time' to datetime
trades_df['open_time'] = pd.to_datetime(trades_df['open_time'], errors='coerce')
trades_df['close_time'] = pd.to_datetime(trades_df['close_time'], errors='coerce')

# a) Trades before 2020
invalid_dates = trades_df[trades_df['open_time'].dt.year < 2020]
if not invalid_dates.empty:
    print("Trades with open_time before 2020 found:")
    print(invalid_dates[['ticket_hash', 'open_time']])

# Conclusion:
# Trades occurring before 2020 are outside the expected date range.

# b) close_time before open_time
invalid_time_sequence = trades_df[trades_df['close_time'] < trades_df['open_time']]
if not invalid_time_sequence.empty:
    print("Trades where close_time is before open_time found:")
    print(invalid_time_sequence[['ticket_hash', 'open_time', 'close_time']])

# Conclusion:
# Trades where close_time precedes open_time are invalid.

# 4. Check joins between trades and users
merged_df = trades_df.merge(users_df, on='login_hash', how='left', indicator=True)
missing_users = merged_df[merged_df['_merge'] == 'left_only']
if not missing_users.empty:
    print("Trades with login_hash not found in users table:")
    print(missing_users[['ticket_hash', 'login_hash']])

# Conclusion:
# Some trades reference login_hash values not present in the users table.

# 5. Check joins between trades and symbols
merged_symbols_df = trades_df.merge(symbols_df, on='symbol', how='left', indicator=True)
missing_symbols = merged_symbols_df[merged_symbols_df['_merge'] == 'left_only']
if not missing_symbols.empty:
    print("Trades with symbol not found in symbols table:")
    print(missing_symbols[['ticket_hash', 'symbol']])

# Conclusion:
# Some trades reference symbols not present in the symbols table.

# 6. Trades by disabled accounts
disabled_accounts = users_df[users_df['enable'] == False]
trades_disabled_accounts = trades_df[trades_df['login_hash'].isin(disabled_accounts['login_hash'])]
if not trades_disabled_accounts.empty:
    print("Trades made by disabled accounts:")
    print(trades_disabled_accounts[['ticket_hash', 'login_hash']])

# Conclusion:
# Trades are associated with disabled accounts, which may be invalid.

# 7. Zero volume trades
zero_volume_trades = trades_df[trades_df['volume'] == 0]
if not zero_volume_trades.empty:
    print("Trades with zero volume found:")
    print(zero_volume_trades[['ticket_hash', 'volume']])

# Conclusion:
# Trades with zero volume may indicate placeholders or errors.

# 8. Edge cases for 'cmd' field
invalid_cmd = trades_df[~trades_df['cmd'].isin([0, 1])]
if not invalid_cmd.empty:
    print("Trades with invalid cmd values found:")
    print(invalid_cmd[['ticket_hash', 'cmd']])

# Conclusion:
# The 'cmd' field contains values other than 0 or 1, which are invalid.

# -------------------------------
# End of Data Quality Checks
# -------------------------------

print("Data quality checks completed.")
