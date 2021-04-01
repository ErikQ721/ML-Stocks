import fmpsdk
import pandas as pd
import numpy as np
import time
import urllib3
import urllib
import datetime
from datetime import timedelta
from tqdm import tqdm
import os

#Step 2: Gathers historical market data about stocks

def preprocess_price_data():
    #Gets the price data from the previous CSVs and cleans them by eliminating weekends 
    sp_dat = pd.read_csv("E:\\PythonCode\\Data\\20210205_Data\\sp500_index.csv", index_col="date", parse_dates=True)
    stock_dat = pd.read_csv("E:\\PythonCode\\Data\\20210205_Data\\stock_prices_All_Long_Clean.csv", index_col="date", parse_dates=True)
    tick_list = stock_dat.columns.values.tolist()

    start_date = sp_dat.index[-1]
    end_date = sp_dat.index[0]
    idx = pd.date_range(start_date, end_date, freq="D")

    sp500_raw_data = sp_dat.reindex(idx)
    stock_raw_data = stock_dat.reindex(idx)

    sp500_raw_data.ffill(inplace=True)

    stock_raw_data.ffill(inplace=True)

    return sp500_raw_data, stock_raw_data, tick_list

def percent_change(current_date,tick_dat,sp_dat):
    #Finds the yearly percentage change of the stock price and the sp500, this is what our prediction will be based off of
    year_ago = current_date - timedelta(days = 366)
    price_now_stk = tick_dat.loc[current_date]
    price_now_sp = sp_dat.loc[current_date,'adjClose']
    

    try:
        price_then_stk = tick_dat.loc[year_ago]
        price_then_sp = sp_dat.loc[year_ago,'adjClose']
    except KeyError:
        price_then_stk = tick_dat.loc[year_ago]
        price_then_sp = sp_dat.loc[year_ago,'adjClose']

    stock_p_change = (price_now_stk - price_then_stk)*100/price_then_stk
    sp_p_change = (price_now_sp - price_then_sp)*100/price_then_sp
    return stock_p_change, sp_p_change

def generate_data(mySTK, sp_dat,tick_dat,apikey):
    #Gathers all the historical data from fmp cloud and joins it into one large database
    #Returns a database for each individual stock
    #If there's an error with the data, just returns an empty database.
    print("") 
    print(mySTK)
    symbol: str = mySTK
    period: str = "quarter"
    
    try:

        prof = pd.DataFrame(fmpsdk.income_statement(apikey=apikey,symbol=symbol, period=period))
        prof.drop(prof.iloc[:,2:6],inplace = True, axis =1)
        prof.drop(prof.columns[-2:],inplace = True, axis =1)

        bal = pd.DataFrame(fmpsdk.balance_sheet_statement(apikey=apikey,symbol=symbol, period=period))
        bal.drop(bal.iloc[:,0:6],inplace = True, axis =1)
        bal.drop(bal.columns[-2:],inplace = True, axis =1)

        CFS = pd.DataFrame(fmpsdk.cash_flow_statement(apikey=apikey,symbol=symbol, period=period))
        CFS.drop(CFS.iloc[:,0:6],inplace = True, axis =1)
        CFS.drop(CFS.columns[-2:],inplace = True, axis =1)

        finRat =  pd.DataFrame(fmpsdk.financial_ratios(apikey=apikey,symbol=symbol, period=period))
        finRat.drop(finRat.iloc[:,0:1],inplace = True, axis =1)

        entVal = pd.DataFrame(fmpsdk.enterprise_values(apikey=apikey,symbol=symbol, period=period))
        entVal.drop(entVal.iloc[:,0:1],inplace = True, axis = 1)

        keyMet = pd.DataFrame(fmpsdk.key_metrics(apikey=apikey,symbol=symbol, period=period))
        keyMet.drop(keyMet.iloc[:,0:1],inplace = True, axis = 1)

        finGrow = pd.DataFrame(fmpsdk.financial_growth(apikey=apikey,symbol=symbol, period=period))
        finGrow.drop(finGrow.iloc[:,0:1],inplace = True, axis = 1)

        combo = pd.concat([prof,bal,CFS,finRat,entVal,keyMet,finGrow],join = 'inner', axis = 1)
        combo.drop(columns=['marketCapitalization','stockPrice'],inplace=True,axis =1)
        combo = combo.loc[:,~combo.columns.duplicated()]
        combo.set_index('date', inplace = True)

        idx = combo.index

        unix_df = pd.DataFrame(index=idx, columns=['Unix'])
        stock_price_df = pd.DataFrame(index=idx,columns=['price'])
        stock_change_df = pd.DataFrame(index=idx,columns=['stock_p_change'])
        sp_price_df = pd.DataFrame(index=idx,columns=['SP500'])
        sp_change_df = pd.DataFrame(index=idx,columns=['SP500_p_change'])
        drop_list = []

        for i in range (len(idx)):
            current_date_str = idx[i]

            current_date = datetime.datetime.strptime(current_date_str, '%Y-%m-%d')

            stock_p_change, sp_p_change = percent_change(current_date,tick_dat,sp_dat)
            unix = time.mktime(current_date.timetuple())
            sp_price = sp_dat.loc[current_date,'adjClose']
            stock_price = tick_dat.loc[current_date]

            unix_df.loc[current_date_str] = unix
            stock_price_df.loc[current_date_str] = stock_price
            stock_change_df.loc[current_date_str] = stock_p_change
            sp_price_df.loc[current_date_str] = sp_price
            sp_change_df.loc[current_date_str] = sp_p_change
            if np.isnan(stock_price):
                drop_list.append(current_date_str)


        metrics = pd.concat([unix_df, stock_price_df,stock_change_df,sp_price_df,sp_change_df],join='inner',axis=1)
        final = pd.concat([metrics,combo],join = 'inner', axis = 1)

        final.drop(drop_list,inplace=True)
    except KeyError:
        final = pd.DataFrame()
        print(mySTK + " is bad bc key")
    except ValueError:
        final = pd.DataFrame()
        print(mySTK + " is bad bc val")
    except urllib.error.URLError:
        final = pd.DataFrame()
        print(mySTK + " is bad bc url")
    except socket.gaierror:
        final = pd.DataFrame()
        print(mySTK + " is bad bc socket")
    
    return final

def parse_data():
    #Gets all the data and joins it together into one large database
    sp_dat, stock_dat, tick_list = preprocess_price_data()
    full_data = pd.DataFrame()
    apikey = '*YOUR API KEY HERE*'

    for i in tqdm(range(len(tick_list)), desc="Parsing progress:", unit="tickers"):
        mySTK = tick_list[i]
        tick_dat = stock_dat[mySTK]
        mystk_dat = generate_data(mySTK, sp_dat,tick_dat,apikey)
        full_data = pd.concat([full_data,mystk_dat],axis = 0, sort=False)
    
    myPath = "E:\\PythonCode\\Data\\20210331_Data"

    #Looks for data path, if not found then it creates it
    if not os.path.exists(myPath):
        print("Creating folder")
        os.makedirs(myPath)
    
    full_data.to_csv(myPath+"\\stock_metrics.csv")

if __name__ == "__main__":
    parse_data()