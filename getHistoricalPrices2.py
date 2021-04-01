import fmpsdk
import pandas as pd
from tqdm import tqdm
import numpy as np

#RUN THIS SCRIPT FIRST
#Step 1: Gets price data from the past about stocks for using later in the code

def build_sp500_dataset(apikey):
    #Pulls data from the fmp cloud api for SPY, a collection of the SP500 stocks, generally a good indicator
    #of how the market is faring
    combo = pd.DataFrame.from_dict(fmpsdk.historical_price_full(apikey=apikey, symbol='SPY'))
    historical = pd.DataFrame(combo['historical'].tolist())
    historical.set_index('date',inplace = True)
    price = historical['adjClose']
    price.to_csv("E:\\PythonCode\\Data\\20210216_Data\\sp500_index.csv")

def build_stock_dataset(apikey):
    #Pulls all the historical stock prices from a selected market.

    #Example markets are "NASDAQ" "NYSE"
    #exchange_dat = pd.DataFrame.from_dict(fmpsdk.exchange_realtime(apikey=apikey, exchange='NYSE'))
    exchange_dat = pd.DataFrame.from_dict(fmpsdk.symbols_list(apikey=apikey))
    stk_list = exchange_dat['symbol']
    stock_dat = pd.DataFrame()

    #len(stk_list)
    #Goes through and finds the historical stock prices for each of the stocks from the exchange
    #Takes a while
    #Also does a preliminary cleaning incase some of the data from the API gets messed up
    for i in tqdm(range(len(stk_list)),desc="Parsing progress:", unit="tickers"):
        tick = stk_list.iat[i]
        print(tick)
        try:
            combo = pd.DataFrame.from_dict(fmpsdk.historical_price_full(apikey=apikey, symbol=tick))
            historical = pd.DataFrame(combo['historical'].tolist())
            historical.set_index('date',inplace = True)
            price = historical['adjClose']
            stock_dat.insert(loc=0, column = tick, value = price)
        except KeyError:
            print(tick + " is bad bc key")
        except ValueError:
            print(tick + " is bad bc value") 

    #Saves the data to a CSV file where ever you specify
    stock_dat = stock_dat.loc[:,~stock_dat.columns.duplicated()]
    stock_dat.to_csv("E:\\PythonCode\\Data\\20210216_Data\\stock_prices.csv")

def clean_data():
    print("Now cleaning data...")
    #cleans the data - if any blank stock prices for the first year in the csv, deletes these columns
    my_data = pd.read_csv("E:\\PythonCode\\Data\\20210216_Data\\stock_prices.csv",index_col="date")
    cols = my_data.columns
    bad_data = []

    for col in range(len(cols)):
        for row in range(486):
            test = np.isnan(my_data.iat[row,col])
            if test:
                bad_data.append(col)
                break
    my_data_clean = my_data.drop(my_data.columns[bad_data],axis = 1)
    my_data_clean.to_csv("E:\\PythonCode\\Data\\20210216_Data\\stock_prices_clean.csv")  



if __name__ == "__main__":
    apikey = "*YOUR API KEY HERE*"
    build_stock_dataset(apikey)
    build_sp500_dataset(apikey)
    clean_data()