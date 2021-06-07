import fmpsdk
import pandas as pd
import numpy as np
import time
import datetime
from datetime import timedelta
from tqdm import tqdm
import os
#Step 2: Gathers historical market data about stocks

def preprocess_price_data(path):
    #Gets the data from the previous CSVs and cleans them by eliminating weekends
    processedPath = path+'\\processedStockData'

    if not os.path.exists(processedPath):
        print("Creating folder")
        os.makedirs(processedPath)

        sp_data = pd.read_csv(path+"\\sp500_index.csv", index_col="date", parse_dates=True)
        stock_data = pd.read_csv(path + "\\bigStockPricesClean.csv", parse_dates=True)

        toDrop = list(range(len(sp_data),len(stock_data)))
        stock_data.drop(toDrop,inplace = True)
        stock_data.index = sp_data.index


        tick_list = stock_data.columns.values.tolist()

        start_date = sp_data.index[-1]
        end_date = sp_data.index[0]
        idx_sp = pd.date_range(start_date, end_date, freq="D")


        sp500_raw_data = sp_data.reindex(idx_sp,method = 'pad')
        stock_raw_data = stock_data.reindex(idx_sp,method = 'pad')

        sp500_raw_data.rename_axis('date',axis = 'index',inplace = True)
        stock_raw_data.rename_axis('date',axis = 'index',inplace = True)
        stock_raw_data.drop(stock_raw_data.columns[[0]],axis = 1,inplace = True)

        sp500_raw_data.to_csv(processedPath+"\\sp500_processed_index.csv")
        stock_raw_data.to_csv(processedPath+"\\stocks_processed_index.csv")
    else:
        print("reading csvs")
        sp500_raw_data = pd.read_csv(processedPath+"\\sp500_processed_index.csv", index_col="date",parse_dates=True)
        stock_raw_data = pd.read_csv(processedPath+"\\stocks_processed_index.csv", index_col="date",parse_dates=True)
        tick_list = stock_raw_data.columns.values.tolist()     


    return sp500_raw_data, stock_raw_data, tick_list

def percent_change(current_date,tick_data,sp_data):
    #Finds the yearly change of the stock price and the sp500
    year_ago = current_date - timedelta(days = 366)
    price_now_stk = tick_data.loc[current_date]
    price_now_sp = sp_data.loc[current_date,'adjClose']
    

    try:
        price_then_stk = tick_data.loc[year_ago]
        price_then_sp = sp_data.loc[year_ago,'adjClose']
    except KeyError:
        price_then_stk = np.nan
        price_then_sp = np.nan
        #price_then_stk = tick_dat.loc[year_ago]
        #price_then_sp = sp_dat.loc[year_ago,'adjClose']

    stock_p_change = (price_now_stk - price_then_stk)*100/price_then_stk
    sp_p_change = (price_now_sp - price_then_sp)*100/price_then_sp
    return stock_p_change, sp_p_change

def generate_data(mySTK, sp_data,tick_data,apikey):
    #Gathers all the historical data from fmp cloud and joins it into one large database
    #Returns a database for each individual stock
    #If there's an error with the data, just returns an empty database.
    print("") 
    print(mySTK)
    symbol: str = mySTK
    period: str = "quarter"
    limit: int = 200

    final = pd.DataFrame()
    prof_drop_list = ['index','reportedCurrency','fillingDate','acceptedDate','period','link','finalLink']
    bal_drop_list = ['index','symbol','reportedCurrency','fillingDate','acceptedDate','period','link','finalLink']
    finRat_drop_list = ['index','symbol','period']
    entVal_drop_list = ['index','symbol']

    final = pd.DataFrame()
    try:
        prof = pd.DataFrame(fmpsdk.income_statement(apikey=apikey,symbol=symbol, period=period,limit=limit))
        if prof.empty:
            print("empty")
            return final
        else:
            prof = prof.reset_index().set_index('date')
            prof.drop(prof_drop_list,axis = 1,inplace = True)

        bal = pd.DataFrame(fmpsdk.balance_sheet_statement(apikey=apikey,symbol=symbol, period=period,limit=limit))
        if bal.empty:
            print("empty")
            return final
        else:
            bal = bal.reset_index().set_index('date')
            bal.drop(bal_drop_list,axis = 1,inplace = True)


        CFS = pd.DataFrame(fmpsdk.cash_flow_statement(apikey=apikey,symbol=symbol, period=period,limit=limit))
        if CFS.empty:
            print("empty")
            return final
        else:
            CFS = CFS.reset_index().set_index('date')
            CFS.drop(bal_drop_list,axis = 1,inplace = True)
            

        finRat =  pd.DataFrame(fmpsdk.financial_ratios(apikey=apikey,symbol=symbol, period=period,limit=limit))
        if finRat.empty:
            print("empty")
            return final
        else:
            finRat = finRat.reset_index().set_index('date')
            finRat.drop(finRat_drop_list,axis = 1,inplace = True)

        entVal = pd.DataFrame(fmpsdk.enterprise_values(apikey=apikey,symbol=symbol, period=period,limit=limit))
        if entVal.empty:
            print("empty")
            return final
        else:
            entVal = entVal.reset_index().set_index('date')
            entVal.drop(entVal_drop_list,axis = 1,inplace = True)

        keyMet = pd.DataFrame(fmpsdk.key_metrics(apikey=apikey,symbol=symbol, period=period,limit=limit))
        if keyMet.empty:
            print("empty")
            return final
        else:
            keyMet = keyMet.reset_index().set_index('date')
            keyMet.drop(finRat_drop_list,axis = 1,inplace = True)

        finGrow = pd.DataFrame(fmpsdk.financial_growth(apikey=apikey,symbol=symbol, period=period,limit=limit))
        if finGrow.empty:
            print("empty")
            return final
        else:
            finGrow = finGrow.reset_index().set_index('date')
            finGrow.drop(finRat_drop_list,axis = 1,inplace = True)

        combo = pd.concat([prof,bal,CFS,finRat,entVal,keyMet,finGrow],join = 'inner', axis = 1)


        idx = combo.index

        unix_df = pd.DataFrame(index=idx, columns=['Unix'])
        stock_price_df = pd.DataFrame(index=idx,columns=['price'])
        stock_change_df = pd.DataFrame(index=idx,columns=['stock_p_change'])
        sp_price_df = pd.DataFrame(index=idx,columns=['SP500'])
        sp_change_df = pd.DataFrame(index=idx,columns=['SP500_p_change'])
        drop_list = []

        for i in range (len(idx)):
            current_date_str = idx[i]
            sp_dt_index = pd.to_datetime(sp_data.index)
            if not (sp_data.index == current_date_str).any():
                break

            current_date = datetime.datetime.strptime(current_date_str, '%Y-%m-%d')

            stock_p_change, sp_p_change = percent_change(current_date,tick_data,sp_data)
            unix = time.mktime(current_date.timetuple())
            sp_price = sp_data.loc[current_date,'adjClose']
            stock_price = tick_data.loc[current_date]

            unix_df.loc[current_date_str] = unix
            stock_price_df.loc[current_date_str] = stock_price
            stock_change_df.loc[current_date_str] = stock_p_change
            sp_price_df.loc[current_date_str] = sp_price
            sp_change_df.loc[current_date_str] = sp_p_change

            if np.isnan(stock_price):
                drop_list.append(current_date_str)
            if np.isnan(stock_p_change):
                drop_list.append(current_date_str)


        metrics = pd.concat([unix_df, stock_price_df,stock_change_df,sp_price_df,sp_change_df],join='inner',axis=1)
        final = pd.concat([metrics,combo],join = 'inner', axis = 1)

        final.drop(drop_list,inplace=True)

    except ValueError:
        final = pd.DataFrame()
        print(mySTK + " is bad bc val")
    
    return final

def parse_data(dataPath,spPath, apikey):

    print("preprocessing")
    sp_data, stock_data, tick_list = preprocess_price_data(spPath)
    print("Starting data gathering")
    full_data = pd.DataFrame()
    saveCount = 200
    myCount = 0
    startCount = 19200
    endCount = len(tick_list)
    #len(tick_list)
    #If having difficulty getting through the entire range, split into groups and use "combine data" script to patch everything together
    for i in tqdm(range(startCount,endCount), desc="Parsing progress:", unit="tickers"):
        mySTK = tick_list[i]
        tick_data = stock_data[mySTK]
        mystk_data = generate_data(mySTK, sp_data,tick_data,apikey)
        full_data = pd.concat([full_data,mystk_data],axis = 0, sort=False)

        if i%saveCount == 0 and not full_data.empty:
            myCount = i
            otherPath = f"\\stock_metrics_{myCount}.csv"
            full_data.to_csv(dataPath+otherPath)
            full_data = pd.DataFrame()

    myCount = myCount+1
    shortPath = f"\\stock_metrics_{myCount}.csv"
    full_data.to_csv(dataPath+shortPath)
    full_data = pd.DataFrame()

def data_string_to_float(number_string):
    """
    The result of our regex search is a number stored as a string, but we need a float.
        - Some of these strings say things like '25M' instead of 25000000.
        - Some have 'N/A' in them.
        - Some are negative (have '-' in front of the numbers).
        - As an artifact of our regex, some values which were meant to be zero are instead '>0'.
    We must process all of these cases accordingly.
    :param number_string: the string output of our regex, which needs to be converted to a float.
    :return: a float representation of the string, taking into account minus sign, unit, etc.
    """
    # Deal with zeroes and the sign
    if ("N/A" in number_string) or ("NaN" in number_string):
        return "N/A"
    elif number_string == ">0":
        return 0
    elif "B" in number_string:
        return float(number_string.replace("B", "")) * 1000000000
    elif "M" in number_string:
        return float(number_string.replace("M", "")) * 1000000
    elif "K" in number_string:
        return float(number_string.replace("K", "")) * 1000
    else:
        return float(number_string)



def runner():
    apikey = #Your API key here
    dataPath = #Where you save your data
    spPath = #WHere your sp500 data is saved

    if not os.path.exists(dataPath):
        print("Creating folder")
        os.makedirs(dataPath)

    parse_data(dataPath,spPath,apikey)
    print("Data Collection Finished")
    

if __name__ == "__main__":
    runner()