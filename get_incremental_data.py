import json
import pandas as pd
import logging
from time import sleep
from datetime import datetime as dt
from datetime import timedelta as td
import dill
import time
from datetime import datetime
#user = json.loads(open('userzerodha.json', 'r').read().rstrip())
#userid=user['user_id']
#password=user['password']
#twofa=user['twofa']


    
from smartapi import smartExceptions
from smartapi import SmartConnect

with open('broker_credentials.json') as f:
  broker_details = json.load(f)

obj=SmartConnect(api_key=broker_details['api']['historical'])
client_id=broker_details['client_id']
password=broker_details['password']
session = obj.generateSession(client_id,password)
refreshToken= session['data']['refreshToken']
obj.getProfile(refreshToken)


def get_historical_data(exchange,scrip,symboltoken,interval,from_date_str,to_date_str):
    
    #print("from date",from_date_str)
    
    from_date=datetime.strptime(from_date_str,'%Y-%m-%d %H:%M')
    to_date=datetime.strptime(to_date_str,'%Y-%m-%d %H:%M')
    threshold_days=1000
    total_days=(to_date-from_date).days
    #print(f"getting historical data for the scrip {scrip} for {total_days} days")
    if total_days > 1000:
     data_series=[]
     [data_series.extend(x) for x in [obj.getCandleData({
     "exchange": exchange,
     "symboltoken": symboltoken,
     "interval": interval,
     "fromdate": from_date_str, 
     "todate": datetime.strftime(to_date,'%Y-%m-%d %H:%M')
     })['data'] for to_date in [to_date-td(days=i*threshold_days) for i in range(round(total_days/threshold_days))]]]
    else:
     data_series=[]
     data_series=obj.getCandleData({
     "exchange": exchange,
     "symboltoken": symboltoken,
     "interval": interval,
     "fromdate": from_date_str, 
     "todate": to_date_str
     })['data']
    historical_data=pd.DataFrame(data_series,columns=['date','open','high','low','close','volume'])
    historical_data.set_index('date',inplace=True)
    historical_data.index=pd.to_datetime(historical_data.index)
    historical_data.sort_index(ascending=False,inplace=True)
    historical_data.drop_duplicates(inplace=True)
    #print(historical_data.head())
    time.sleep(0.5)
    return historical_data
    
  
#fetch the feedtoken
feedToken=obj.getfeedToken()

#fetch User Profile
userProfile= obj.getProfile(refreshToken)


exchange="NSE"
symboltoken="3045"
scrip="SBIN"
interval= "ONE_DAY"
from_date_str="2008-01-01 00:00"
to_date_str=datetime.strftime(datetime.today(),'%Y-%m-%d %H:%M')
dim_scrip_file='dim_scrips.csv'
dim_scrips=pd.read_csv(dim_scrip_file)
scrips_to_get_data=dim_scrips.loc[dim_scrips['get_incremental']=='Y']

if __name__ == '__main__':
 for i,row in scrips_to_get_data.iterrows():
   
   try:  
    
    exchange="NSE"
    symboltoken=row['exchange_token']
    scrip=row['Symbol']
    print(f"Getting incremental data for the scrip {scrip}")  
    interval= "ONE_DAY"
    from_date_str=row['last_refresh_date']
    historical_data=pd.read_parquet(f'data/{scrip}.parquet')
    if str(from_date_str)=='nan':
       from_date_str=datetime.strftime(historical_data.index.max(),'%Y-%m-%d %H:%M')
    elif '/' in from_date_str:
       from_date_str=datetime.strftime(datetime.strptime(from_date_str,'%m/%d/%Y %H:%M'),'%Y-%m-%d %H:%M')
       
    to_date_str=datetime.strftime(datetime.today(),'%Y-%m-%d %H:%M')
    data=get_historical_data(exchange,scrip,symboltoken,interval,from_date_str,to_date_str)
    #print(data)

    
    
    if len(data)!=0:
     data=pd.concat([data,historical_data])
    
    
     #data.set_index('date',inplace=True)
     data.index=pd.to_datetime(data.index)
     data.sort_index(ascending=False,inplace=True)
     #data.index.drop_duplicates(keep='last')
     data.drop_duplicates(inplace=True)
     dim_scrips.loc[dim_scrips.Symbol==scrip,'last_refresh_date']=datetime.strftime(data.index.max(),'%Y-%m-%d %H:%M')
     dim_scrips.to_csv(dim_scrip_file,index=False)
    
     data.to_parquet(f'data/{scrip}.parquet')
    else:
        
     dim_scrips.loc[dim_scrips.Symbol==scrip,'last_refresh_date']=datetime.strftime(historical_data.index.max(),'%Y-%m-%d %H:%M')
     dim_scrips.to_csv(dim_scrip_file,index=False)
    
   except smartExceptions.DataException as e:
       print("Data Exception",e)
       raise  
       