import pandas as pd
import matplotlib.pyplot as plt

# Load the data
file_path = 'dow_jones_5min_data.xlsx'
data = pd.read_excel(file_path)

# Clean and process data
data = data.drop([0, 1]).reset_index(drop=True)  # Drop the header rows and reset index
data.columns = ['Datetime', 'Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume']
data['Datetime'] = pd.to_datetime(data['Datetime'])
data[['Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume']] = data[
    ['Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume']
].apply(pd.to_numeric, errors='coerce')

# Define a function to calculate the MACD, Signal Line, and Histogram
def calculate_macd(data, short_period=12, long_period=26, signal_period=9):
    # Calculate the short-term and long-term EMAs
    data['EMA_short'] = data['Adj Close'].ewm(span=short_period, adjust=False).mean()
    data['EMA_long'] = data['Adj Close'].ewm(span=long_period, adjust=False).mean()
    
    # Calculate the MACD line (DIF)
    data['MACD'] = data['EMA_short'] - data['EMA_long']
    
    # Calculate the Signal line (DEA)
    data['Signal'] = data['MACD'].ewm(span=signal_period, adjust=False).mean()
    
    # Calculate the Histogram
    data['Histogram'] = data['MACD'] - data['Signal']
    
    return data

# Apply the MACD calculation function
data = calculate_macd(data)

# Identify Golden Crosses and Death Crosses
data['Signal_Cross'] = 0  # Initialize a column to store the cross signals
data.loc[(data['MACD'] > data['Signal']) & (data['MACD'].shift(1) <= data['Signal'].shift(1)), 'Signal_Cross'] = 1  # Golden Cross
data.loc[(data['MACD'] < data['Signal']) & (data['MACD'].shift(1) >= data['Signal'].shift(1)), 'Signal_Cross'] = -1  # Death Cross

output_file_path = 'dow_jones_5min_macd.xlsx'
data.to_excel(output_file_path, index=False)
# Plot the data
plt.figure(figsize=(14, 8))

# Plot the adjusted close price
plt.subplot(2, 1, 1)
plt.plot(data['Datetime'], data['Adj Close'], label='Adj Close', color='blue')
plt.title('Adjusted Close Price and MACD')
plt.xlabel('Date')
plt.ylabel('Price')
plt.legend()

# Plot the MACD and Signal line
plt.subplot(2, 1, 2)
plt.plot(data['Datetime'], data['MACD'], label='MACD', color='orange')
plt.plot(data['Datetime'], data['Signal'], label='Signal Line', color='green')
plt.bar(data['Datetime'], data['Histogram'], label='Histogram', color='gray', alpha=0.3)

# Mark Golden Cross and Death Cross
plt.scatter(data.loc[data['Signal_Cross'] == 1, 'Datetime'], data.loc[data['Signal_Cross'] == 1, 'MACD'], 
            marker='^', color='green', label='Golden Cross', s=100)
plt.scatter(data.loc[data['Signal_Cross'] == -1, 'Datetime'], data.loc[data['Signal_Cross'] == -1, 'MACD'], 
            marker='v', color='red', label='Death Cross', s=100)

plt.xlabel('Date')
plt.ylabel('MACD')
plt.legend()
plt.tight_layout()
plt.show()
