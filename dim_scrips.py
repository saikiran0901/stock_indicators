# -*- coding: utf-8 -*-
"""
Created on Wed Jun  2 16:37:20 2021

@author: sravula
"""

import pandas as pd

nse_sheet=pd.read_csv("nse_mcap.csv")
eq_instruments=pd.read_csv("eq_instruments.csv")

dim_script=nse_sheet

df_merge_col=pd.merge(nse_sheet,eq_instruments,left_on='symbol',right_on='tradingsymbol',how='inner')
#df_merge_col['mcap_cr_1']=pd.to_numeric(df_merge_col['mcap_cr'])

threshold_mcap=2000
filtered_df=df_merge_col[df_merge_col['mcap_lakhs'].astype(int)>=threshold_mcap*100]

filtered_df.to_csv('dim_scrips.csv',index=False)