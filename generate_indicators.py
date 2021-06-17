import pandas as pd
import numpy as np
import talib

#data=pd.read_csv("data/RELIANCE.csv")

indicators_csv=pd.DataFrame(columns=['exchange','scrip','scrip_name','all_time_high','all_time_low','ltp','uptrend','Fib Support','Fib Resistance','macd_signal','MACD_alert','psar_signal','PSAR_alert','ichmoku_signal_1','ICHMOKU_alert_1','ichmoku_signal_2','ICHMOKU_alert_2','analysis_last_candle'])
dim_scrips=pd.read_csv('dim_scrips.csv')
indicators_raw_data=pd.DataFrame(columns=['exchange','scrip','scrip_name','open','high','low','close','volume','lead_span_A','lead_span_B','lagging_span'])
i=0
total_scrips=len(dim_scrips)
try:
 for i,row in dim_scrips.iterrows():
   if row['analysis']=='Y':  
    
    scrip=row['Symbol']
    scrip_name=row['Company_Name']
    #print(f"Performing analysis for the scrip {scrip}")
    #print(f"Analysing scrip {scrip}")
    data=pd.read_parquet(f"data/{scrip}.parquet")
    #data.index=pd.to_datetime(data['date'])
    
    
    #date_index_without_tz=data.index.tz_convert(None)
    date_index_without_tz=data.index.tz_localize(None)
    data.index=pd.to_datetime(date_index_without_tz)
    data.index=pd.to_datetime(data.index)
    data.sort_index(ascending=True,inplace=True)
    data.set_index(data.index,inplace=True)
    data.drop_duplicates(inplace=True)
    #print(data.tail())
    data['scrip']=scrip
    data['scrip_name']=scrip_name
    max_index=data.index.max()
    #print("max index",data.index.max())
    scrip_ltp=data.loc[str(data.index.max())]['close'].max()
    data['exchange']="NSE"    
    scrip_max=data['high'].max()
    scrip_min=data['low'].min()
    scrip_max_date=data.loc[data['high']==scrip_max].index.max()
    scrip_min_date=data.loc[data['low']==scrip_min].index.min()
    uptrend=False
    if scrip_max_date > scrip_min_date:
        uptrend=True
    #print("Scrip in uptrend is ",uptrend)
        
    diff=scrip_max-scrip_min
    fib_ratios= [0,0.236, 0.382, 0.5 , 0.618, 0.786,1]
    if uptrend:
     fib_levels=[scrip_max-(diff*ratio) for ratio in fib_ratios] #up trend
     fib_support=[supp for supp in fib_levels if supp < scrip_ltp][0]
    else:
     fib_levels=[scrip_min+(diff*ratio) for ratio in fib_ratios] #down trend  
     fib_support=[supp for supp in fib_levels if supp < scrip_ltp][-1]
    #print(fib_levels)
    
    fib_resistance=[res for res in fib_levels if res > scrip_ltp][0]
    #print("Support",fib_support,"Res",fib_resistance)
    
###############ichmoku cloud##################    
    # Define length of Tenkan Sen or Conversion Line
    cl_period_slow = 9
    cl_period_fast = 5
    
    # Define length of Kijun Sen or Base Line
    bl_period_slow = 9
    bl_period_fast = 5
    
    # Define length of Senkou Sen B or Leading Span B
    lead_span_b_period = 120  
    
    # Define length of Chikou Span or Lagging Span
    lag_span_period = 30  
    
    # Calculate conversion line
    high_slow = data['high'].rolling(cl_period_slow).max()
    low_slow = data['low'].rolling(cl_period_slow).min()
    data['conversion_line_slow'] = (high_slow + low_slow) / 2
    
    high_fast = data['high'].rolling(cl_period_fast).max()
    low_fast = data['low'].rolling(cl_period_fast).min()
    data['conversion_line_fast'] = (high_fast + low_fast) / 2
    
    # Calculate based line
    high_slow = data['high'].rolling(bl_period_slow).max()
    low_slow = data['low'].rolling(bl_period_slow).min()
    data['base_line_slow'] = (high_slow + low_slow) / 2
    
    high_fast = data['high'].rolling(bl_period_fast).max()
    low_fast = data['low'].rolling(bl_period_fast).min()
    data['base_line_fast'] = (high_fast + low_fast) / 2
    
    # Calculate leading span A
    data['lead_span_A'] = ((data.conversion_line_slow + data.base_line_slow) / 2).shift(lag_span_period)
    
    # Calculate leading span B
    high_120 = data['high'].rolling(120).max()
    low_120 = data['high'].rolling(120).min()
    data['lead_span_B'] = ((high_120 + low_120) / 2).shift(lead_span_b_period)
    
    # Calculate lagging span
    data['lagging_span'] = data['close'].shift(-lag_span_period)
    
    
##################macd###################

    data['MACD'],data['MACDsignal'],data['MACDhist'] = talib.MACD(data.close, fastperiod=12, slowperiod=26, signalperiod=9)    


#################psar###################
    
    data['SAR'] = talib.SAR(data.high, data.low, acceleration=0.02, maximum=0.2)
    
    data.loc[(data.close>data.SAR),'SAR_signal']='BUY'
    data.loc[(data.close<data.SAR),'SAR_signal']='SELL'
    
    data.loc[(data.MACD>data.MACDsignal),'MACD_signal']='BUY'
    data.loc[(data.MACD<data.MACDsignal),'MACD_signal']='SELL'
        
    
    data.loc[(((data.close>data.lead_span_A) & (data.lead_span_A>data.lead_span_B) & 
             (1.01*data.lead_span_A>data.close) & (data.close>data.lead_span_A))),'ICHMOKU_signal_1']='BUY'
    
    data.loc[(((data.conversion_line_slow<data.base_line_slow) & 
             (data.conversion_line_fast>data.base_line_fast) &
             (data.close>data.lead_span_A) & (data.lead_span_A>data.lead_span_B))),'ICHMOKU_signal_2']='BUY'

    data.loc[(((data.close<data.lead_span_A) & (data.lead_span_B>data.lead_span_A) &
             (1.01*data.lead_span_A<data.close) & (data.close<data.lead_span_A))),'ICHMOKU_signal_1']='SELL'
    
    data.loc[(((data.conversion_line_slow>data.base_line_slow) & 
             (data.conversion_line_fast<data.base_line_fast) 
             & (data.close<data.lead_span_A) & (data.lead_span_B>data.lead_span_A))),'ICHMOKU_signal_2']='SELL'
             
    
    #shift alerts
    
    data['SAR_signal_1']=data['SAR_signal'].shift(1)
    data['MACD_signal_1']=data['MACD_signal'].shift(1)
    data['ICHMOKU_signal_1_1']=data['ICHMOKU_signal_1'].shift(1)
    data['ICHMOKU_signal_2_1']=data['ICHMOKU_signal_2'].shift(1)
    
    data.loc[(data.SAR_signal_1!=data.SAR_signal) & (pd.notnull(data.SAR_signal)),'SAR_alert']='TRUE'
    data.loc[(data.MACD_signal_1!=data.MACD_signal) & (pd.notnull(data.MACD_signal)),'MACD_alert']='TRUE'
    data.loc[(data.ICHMOKU_signal_1_1!=data.ICHMOKU_signal_1) & (pd.notnull(data.ICHMOKU_signal_1)),'ICHMOKU_alert_1']='TRUE'
    data.loc[(data.ICHMOKU_signal_2_1!=data.ICHMOKU_signal_2) & (pd.notnull(data.ICHMOKU_signal_2)),'ICHMOKU_alert_2']='TRUE'
    
    sar_signal=data.loc[data.index.max()]['SAR_signal']    
    macd_signal=data.loc[data.index.max()]['MACD_signal']         
    ichmoku_signal_1=data.loc[data.index.max()]['ICHMOKU_signal_1']
    ichmoku_signal_2=data.loc[data.index.max()]['ICHMOKU_signal_2']
    
    SAR_alert=data.loc[data.index.max()]['SAR_alert']
    MACD_alert=data.loc[data.index.max()]['MACD_alert']    
    ICHMOKU_alert_1=data.loc[data.index.max()]['ICHMOKU_alert_1']
    ICHMOKU_alert_2=data.loc[data.index.max()]['ICHMOKU_alert_2']
    
    indicators_csv.loc[i]="NSE",scrip,scrip_name,scrip_max,scrip_min,scrip_ltp,uptrend,fib_support,fib_resistance,macd_signal,MACD_alert,sar_signal,SAR_alert,ichmoku_signal_1,ICHMOKU_alert_1,ichmoku_signal_2,ICHMOKU_alert_2,max_index
    #alerts_csv.loc[i]="NSE",scrip,scrip_name,scrip_max,scrip_min,scrip_ltp,uptrend,fib_support,fib_resistance,macd_signal,sar_signal,ichmoku_signal_1,ichmoku_signal_2,max_index
    #data.to_csv("ichmoku_cloud.csv")
    
    indicators_raw_data=pd.concat([indicators_raw_data,data['2021']],sort=True)
    print(f"Analysis Completed {round((i+1)/total_scrips*100,2)} % ",end="\r")
    #i=i+1
    
    
except Exception as e:
    print("Exception",e)

#upload_to_drive()
indicators_csv.to_excel("indicators.xlsx",index=False)
print("\nGenerated fibonacci")
indicators_raw_data.sort_index(ascending=False,inplace=True)
print("Generated indicators raw data")
indicators_raw_data.to_csv("indicators_raw_data.csv")
