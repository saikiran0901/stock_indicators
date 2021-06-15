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

    
   
#historicParam={
#"exchange":"NSE",
#"symboltoken":"3045",
#"interval": "ONE_MINUTE",
#"fromdate":"2021-04-19 09:15",
#"todate":"2021-04-19 15:30"
#}
            
#user=get_broker_details('zerodha')
#kite = KiteExt()
#kite.login_with_credentials(
#    userid=user['client_id'], password=user['password'], twofa=user['twofa'])
#print(kite.profile())
#
#z=zerodha_data()
#from_date=datetime.strptime("2008-01-01",'%Y-%m-%d')
#to_date=datetime.strptime("2021-05-01",'%Y-%m-%d')
#token_dict={'HDFCBANK':341249,'RELIANCE':738561,'TCS':2953217}
#
#data=z.get_historical_data(kite,2953217,from_date,to_date,"day")
#data.to_csv("data/TCS.csv",index=False)



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
    
    from_date=datetime.strptime(from_date_str,'%Y-%m-%d %H:%M')
    to_date=datetime.strptime(to_date_str,'%Y-%m-%d %H:%M')
    threshold_days=1000
    total_days=(to_date-from_date).days
    print(f"getting historical data for the scrip {scrip} for {total_days} days")

    data_series=[]
    [data_series.extend(x) for x in [obj.getCandleData({
    "exchange": exchange,
    "symboltoken": symboltoken,
    "interval": interval,
    "fromdate": from_date_str, 
    "todate": datetime.strftime(to_date,'%Y-%m-%d %H:%M')
    })['data'] for to_date in [to_date-td(days=i*threshold_days) for i in range(round(total_days/threshold_days))]]]
    historical_data=pd.DataFrame(data_series,columns=['date','open','high','low','close','volume'])
    historical_data.set_index('date',inplace=True)
    historical_data.index=pd.to_datetime(historical_data.index)
    historical_data.sort_index(ascending=False,inplace=True)
    historical_data.drop_duplicates(inplace=True)
    #print(historical_data.head())
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

dim_scrips=pd.read_csv('dim_scrips.csv')
scrips_to_get_data=dim_scrips.loc[dim_scrips['get_historical']=='Y']

if __name__ == '__main__':
 for i,row in scrips_to_get_data.iterrows():
    time.sleep(5)
    exchange="NSE"
    symboltoken=row['exchange_token']
    scrip=row['Symbol']
    interval= "ONE_DAY"
    from_date_str="2008-01-01 00:00"
    to_date_str=datetime.strftime(datetime.today(),'%Y-%m-%d %H:%M')    
    data=get_historical_data(exchange,scrip,symboltoken,interval,from_date_str,to_date_str)
    data.to_parquet(f'data/{scrip}.parquet')