#for symbol in symbol_list:    
#    path = wd + f'/data/{symbol}/daily_{symbol}.csv'
#    print(path)

import os
from functools import reduce
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date
from alpha_vantage.timeseries import TimeSeries
from time import sleep
import glob


#import api_key_alpha in your main program/notebook by "import myalpha"
#all data retrieved from alpha_vantage is in form of csv and is also stored as csv on a local path
#local path follows this nameconvention: <working-directory>/data/<symbol>/daily_<symbol>.csv
#find your working directory by using this code: import os;  wd  = os.getcwd()
#your working directory is where you write your python notebook in Jupyter lab / Jupyter notebook
#example: for procter&gamble ticker symbol csv is stored in <woring directory path>/data/PG/daily_PG.csv
#you can create a "data" folder if you want if not it will be created for you


#get all daily ticker symbol data for symbol_list from alpha-vantage
#need to rewrite this code: create list of paths, and list of dataframes
#use for path,_df in zip(path_list,df_list) to do _df.to_csv - this should work
def get_alphav_10symbols(symbol_list,api_key_alpha,function='TIME_SERIES_DAILY_ADJUSTED'):
    num = len(symbol_list)
    if num > 20: raise Exception("no more than 10 symbols allowed")
    #myau.get_alphav_all(symbol_list, api_key_alpha, function)
    _dict = {}

    for symbol in symbol_list:        
            _dict[symbol]=get_alphav_symbol(symbol,api_key_alpha,function)
            print("_____________________________")
            print(symbol+" "+str(len(_dict)))
    write_csv_one_by_one(_dict)
    return   
    

def write_csv_one_by_one(_dict):
    _df_list = []
    _symbol_list = []
    counter = 0
    for symbol, _df in _dict.items():
        counter += counter + 1
        dirName = './data/{0}'.format(symbol)
        _df.index = pd.to_datetime(_df.index)
        _df.sort_values(by=['timestamp'],axis='index',ascending=True,inplace=True)
        if not os.path.exists(dirName):
            os.mkdir(dirName)
            print("Directory " , dirName ,  " Created ")
        else:    
            print("Directory " , dirName ,  " already exists")
           
        write_csv_one_by_one_df(symbol, _df, counter) 
        
    _modTimesinceEpoc = os.path.getmtime('./data/{0}/daily_{1}.csv'.format(symbol,symbol))
    _modificationTime = datetime.fromtimestamp(_modTimesinceEpoc).strftime('%Y-%m-%d %H:%M:%S')
    print("Last Modified Time : ", _modificationTime)
    del _dict
    return


def write_csv_one_by_one_df(symbol, _df, counter):
    if counter == 1:
            df1 = _df
            df1.to_csv('./data/{0}/daily_{1}.csv'.format(symbol,symbol))
    elif counter == 2:
            df2 = _df
            df2.to_csv('./data/{0}/daily_{1}.csv'.format(symbol,symbol))
    elif counter == 3:
            df3 = _df
            df3.to_csv('./data/{0}/daily_{1}.csv'.format(symbol,symbol))
    elif counter == 4:
            df4 = _df
            df4.to_csv('./data/{0}/daily_{1}.csv'.format(symbol,symbol))
    elif counter == 5:
            df5 = _df
            df5.to_csv('./data/{0}/daily_{1}.csv'.format(symbol,symbol))
    elif counter == 6:
            df6 = _df
            df6.to_csv('./data/{0}/daily_{1}.csv'.format(symbol,symbol))
    elif counter == 7:
            df7 = _df
            df7.to_csv('./data/{0}/daily_{1}.csv'.format(symbol,symbol))
    elif counter == 8:
            df8 = _df
            df8.to_csv('./data/{0}/daily_{1}.csv'.format(symbol,symbol))
    elif counter == 9:
            df9 = _df
            df9.to_csv('./data/{0}/daily_{1}.csv'.format(symbol,symbol))
    elif counter == 10: