import fmpsdk
import pandas as pd
from tqdm import tqdm
import numpy as np
import os

#Step 1: Gets price data from the past about stocks for using later in the code

def build_sp500_dataset(path):
    #Pulls data from the fmp cloud api for SPY, a collection of the SP500 stocks, generally a good indicator
    #of how the market is faring
    apikey = #YOUR API KEY HERE
    myPath = path + "sp500_index.csv"
    combo = pd.DataFrame.from_dict(fmpsdk.historical_price_full(apikey=apikey, symbol='SPY',from_date='1980-01-01'))
    historical = pd.DataFrame(combo['historical'].tolist())
    historical.set_index('date',inplace = True)
    price = historical['adjClose']
    price.to_csv(myPath)

def build_stock_dataset(path):
    #Pulls all the historical stock prices from a selected market.

    #If your comp has enough ram/processing power, delete the market type to get all stock data from the FMP Clouds library
    apikey = #YOUR API KEY HERE
    #exchange_dat = pd.DataFrame.from_dict(fmpsdk.exchange_realtime(apikey=apikey, exchange='NYSE'))
    exchange_dat = pd.DataFrame.from_dict(fmpsdk.symbols_list(apikey=apikey))
    stk_list = exchange_dat['symbol']
    stock_dat = pd.DataFrame()

    #len(stk_list)
    #Goes through and finds the historical stock prices for each of the stocks from the exchange
    #Takes a while
    #Also does a preliminary cleaning incase some of the data from the API gets messed up
    myCount = 17
    for i in tqdm(range(28000,len(stk_list)),desc="Parsing progress:", unit="tickers"):
        tick = stk_list.iat[i]
        print(tick)
        try:
            combo = pd.DataFrame.from_dict(fmpsdk.historical_price_full(apikey=apikey, symbol=tick,from_date='1980-01-01'))
            historical = pd.DataFrame(combo['historical'].tolist())
            historical.set_index('date',inplace = True)
            price = historical['adjClose']
            stock_dat.insert(loc=0, column = tick, value = price)
        except KeyError:
            print(tick + " is bad bc key")
        except ValueError:
            print(tick + " is bad bc value")

        if i%2000 == 0:
            myCount = myCount+1
            finalPath =f"\\stock_prices_{myCount}.csv"
            stock_dat = stock_dat.loc[:,~stock_dat.columns.duplicated()]
            stock_dat.to_csv(path + finalPath)
            stock_dat = pd.DataFrame()
    
    myCount = myCount + 1
    finalPath =f"\\stock_prices_{myCount}.csv"
    stock_dat = stock_dat.loc[:,~stock_dat.columns.duplicated()]
    stock_dat.to_csv(path + finalPath)


    #Saves the data to a CSV file where ever you specify. Be sure to rename your csv to the market that you got it from

def clean_data(path):
    oldPath = path +"\\bigStockPrices.csv"
    newPath = path + "\\bigStockPricesclean.csv"
    print("Now cleaning data...")
    #cleans the data - if any blank stock prices for the first year in the csv, deletes these columns
    my_data = pd.read_csv(oldPath,index_col="date",parse_dates=True)
    #print(my_data)
    cols = my_data.columns
    bad_data = []

    for col in tqdm(range(len(cols)),desc="cleaning", unit="tickers"):
        for row in range(5):
            try:
                test = np.isnan(my_data.iat[row,col])
                if test:
                    bad_data.append(col)
                    break
            except TypeError:
                print(cols[col])
                bad_data.append(col)
    my_data_clean = my_data.drop(my_data.columns[bad_data],axis = 1)
    my_data_clean.to_csv(newPath)
    print("Cleaning Finished")  

def runner():
    path = "E:\\PythonCode\\Data\\20210407_Data\\stockPriceData"
    datPath = "E:\\PythonCode\\Data\\20210407_Data"
    if not os.path.exists(path):
        print("Creating folder")
        os.makedirs(path)
    
    #build_sp500_dataset(path)
    #build_stock_dataset(path)
    clean_data(datPath)



if __name__ == "__main__":
    runner()