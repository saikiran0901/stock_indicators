import pandas as pd
import numpy as np

#data=pd.read_csv("data/RELIANCE.csv")

import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf

df = yf.download('BTC-USD', '2019-01-01', '2021-01-01')

df=pd.read_csv("data/RELIANCE.csv")
df.index = pd.to_datetime(df['date'])
# Define length of Tenkan Sen or Conversion Line
cl_period = 20 

# Define length of Kijun Sen or Base Line
bl_period = 60  

# Define length of Senkou Sen B or Leading Span B
lead_span_b_period = 120  

# Define length of Chikou Span or Lagging Span
lag_span_period = 30  

# Calculate conversion line
high_20 = df['high'].rolling(cl_period).max()
low_20 = df['low'].rolling(cl_period).min()
df['conversion_line'] = (high_20 + low_20) / 2

# Calculate based line
high_60 = df['high'].rolling(bl_period).max()
low_60 = df['low'].rolling(bl_period).min()
df['base_line'] = (high_60 + low_60) / 2

# Calculate leading span A
df['lead_span_A'] = ((df.conversion_line + df.base_line) / 2).shift(lag_span_period)

# Calculate leading span B
high_120 = df['high'].rolling(120).max()
low_120 = df['high'].rolling(120).min()
df['lead_span_B'] = ((high_120 + low_120) / 2).shift(lead_span_b_period)

# Calculate lagging span
df['lagging_span'] = df['close'].shift(-lag_span_period)

# Drop NA values from Dataframe
df.dropna(inplace=True) 

# Add figure and axis objects
fig, ax = plt.subplots(1, 1, sharex=True, figsize=(20, 9))

# Plot close with index on x-axis with a line thickness of 4
ax.plot(df.index, df['close'], linewidth=4)

# Plot Leading Span A with index on the shared x-axis
ax.plot(df.index, df['lead_span_A'])

# Plot Leading Span B with index on the sahred x-axis
ax.plot(df.index, df['lead_span_B'])

# Use the fill_between of ax object to specify where to fill
ax.fill_between(df.index, df['lead_span_A'], df['lead_span_B'],
                where=df['lead_span_A'] >= df['lead_span_B'], color='lightgreen')

ax.fill_between(df.index, df['lead_span_A'], df['lead_span_B'],
                where=df['lead_span_A'] < df['lead_span_B'], color='lightcoral')

plt.legend(loc=0)
plt.grid()
plt.show()

