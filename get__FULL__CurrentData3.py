
from datetime import date,timedelta
import pandas as pd
import numpy as np
from tqdm import tqdm
from combineMetricsData3 import combineData
from get__FULL__HistoricalData3 import preprocess_price_data
import fmpsdk
import os

#Step 3: finds the most current data available

def setCurrentDate():
    global day_now
    day_now = pd.to_datetime(date.today())

def getCurrentDate():
    global day_now
    return day_now


def percentChange(tick_data,currentPrice):
    #finds how much the stock has changed in the last year
    currentDate = getCurrentDate()
    lastQ = pd.to_datetime(currentDate - timedelta(days = 366))

    try:
        price_then_stk = tick_data.loc[lastQ]
    except KeyError:
        stock_p_change = np.nan
        return stock_p_change


    stock_p_change = (currentPrice- price_then_stk)*100/price_then_stk
    return stock_p_change

def setSpyData(apikey,spyData):
    #Finds the current data for the sp500
    global currSpyPrice
    global currSpyChange
    currentDate = getCurrentDate()
    lastQ = pd.to_datetime(currentDate - timedelta(days = 366))

    spy_bigDat = pd.DataFrame.from_dict(fmpsdk.historical_price_full(apikey=apikey, symbol='SPY'))
    historical = pd.DataFrame(spy_bigDat['historical'].tolist())
    currSpyPrice = historical.iat[0,5]
    oldSpyPrice = spyData.loc[lastQ]
    currSpyChange = (currSpyPrice - oldSpyPrice)*100/oldSpyPrice

def getSpyData():
    global currSpyPrice
    global currSpyChange
    return currSpyPrice,currSpyChange

def parse_data(apikey,dataPath,spPath,locPath):
    #Steps through the stocks and finds the most current data for them
    print("preprocessing")
    setCurrentDate()
    sp_data, stock_data, tick_list = preprocess_price_data(spPath)
    setSpyData(apikey,sp_data)

    old_data = combineData(dataPath)
    tick_names = old_data['symbol'].drop_duplicates()
    full_data = pd.DataFrame()


    saveCount = 200
    myCount = 0
    startCount = myCount*saveCount

    savePath = locPath+'\\currentData'
    if not os.path.exists(savePath):
        print("Creating folder")
        os.makedirs(savePath)

    for i in tqdm(range(startCount,len(tick_names)), desc="Parsing progress:", unit="tickers"):

        stock = tick_names[i]
        print(stock)
        tick_data = stock_data[stock]
        lastStockData = pd.DataFrame()
        myStockData = old_data.loc[old_data['symbol']==stock]
        lastStockData = myStockData.head(1)
        stockDataToAdd = pd.DataFrame()

        idx = myStockData.index
        idxTime = pd.to_datetime(idx[0])
        startDate = pd.to_datetime('12/15/2020')

        if(idxTime > startDate):
            stockDataToAdd = generateData(apikey,stock,lastStockData,tick_data)
            full_data = pd.concat([full_data,stockDataToAdd],axis = 0, sort=False)
        
        if i%saveCount == 0 and not full_data.empty:
            myCount = myCount+1
            otherPath = f"\\currentData_{myCount}.csv"
            full_data.to_csv(savePath+otherPath)
            full_data = pd.DataFrame()

def generateData(apikey,myStock,myStockData,tickPriceData):
    
    #where the data is generated, finds the latest date and then updates all metrics that can be updated
    symbol = myStock
    day_now = getCurrentDate()
    spyPrice, spyChange = getSpyData()
    try:
        myStockPriceData = pd.DataFrame.from_dict(fmpsdk.historical_price_full(apikey=apikey, symbol=symbol))
        stockHist = pd.DataFrame(myStockPriceData['historical'].tolist())
        myStockPrice = stockHist.iat[0,5]

        stock_p_change = percentChange(tickPriceData, myStockPrice)

        fin_ratTTM = pd.DataFrame.from_dict(fmpsdk.financial_ratios_ttm(apikey=apikey, symbol=symbol))
        key_metTTM = pd.DataFrame.from_dict(fmpsdk.key_metrics_ttm(apikey=apikey, symbol=symbol))

        combo = pd.concat([fin_ratTTM,key_metTTM],axis = 1)


        combo.index = [day_now]
        combo.index.rename('date',inplace = True)
        combo = combo.loc[:,~combo.columns.duplicated()]
        combo.drop(combo.iloc[:,[0,1]],inplace = True,axis = 1)

        hider = True

        while hider is True:

            myStockData.iat[0,1] = myStockPrice
            myStockData.iat[0,2] = stock_p_change
            myStockData.iat[0,3] = spyPrice
            myStockData.iat[0,4] = spyChange

            myStockData.iat[0,172] = combo.iat[0,0]
            myStockData.iat[0,135] = combo.iat[0,1]
            myStockData.iat[0,191] = combo.iat[0,1]
            myStockData.iat[0,101] = combo.iat[0,2]
            myStockData.iat[0,187] = combo.iat[0,2]
            myStockData.iat[0,102] = combo.iat[0,3]
            myStockData.iat[0,103] = combo.iat[0,4]
            myStockData.iat[0,104] = combo.iat[0,5]
            myStockData.iat[0,105] = combo.iat[0,6]
            myStockData.iat[0,106] = combo.iat[0,7]
            myStockData.iat[0,107] = combo.iat[0,8]
            myStockData.iat[0,108] = combo.iat[0,9]
            myStockData.iat[0,109] = combo.iat[0,10]
            myStockData.iat[0,110] = combo.iat[0,11]
            myStockData.iat[0,111] = combo.iat[0,12]
            myStockData.iat[0,112] = combo.iat[0,13]
            myStockData.iat[0,113] = combo.iat[0,14]
            myStockData.iat[0,114] = combo.iat[0,15]
            myStockData.iat[0,115] = combo.iat[0,16]
            myStockData.iat[0,116] = combo.iat[0,17]
            myStockData.iat[0,117]=combo.iat[0,18]
            myStockData.iat[0,118]=combo.iat[0,19]
            myStockData.iat[0,119] = combo.iat[0,20]
            myStockData.iat[0,120]=combo.iat[0,21]
            myStockData.iat[0,121]=combo.iat[0,22]
            myStockData.iat[0,122]=combo.iat[0,23]
            myStockData.iat[0,123]=combo.iat[0,24]
            myStockData.iat[0,124]=combo.iat[0,25]
            myStockData.iat[0,188]=combo.iat[0,25]
            myStockData.iat[0,125]=combo.iat[0,26]
            myStockData.iat[0,126]=combo.iat[0,27]
            myStockData.iat[0,127]=combo.iat[0,28]
            myStockData.iat[0,213]=combo.iat[0,28]
            myStockData.iat[0,128]=combo.iat[0,29]
            myStockData.iat[0,214]=combo.iat[0,29]
            myStockData.iat[0,129]=combo.iat[0,30]
            myStockData.iat[0,215]=combo.iat[0,29]
            myStockData.iat[0,130]=combo.iat[0,31]
            myStockData.iat[0,131]=combo.iat[0,32]
            myStockData.iat[0,132]=combo.iat[0,33]
            myStockData.iat[0,163]=combo.iat[0,33]
            myStockData.iat[0,133]=combo.iat[0,34]
            myStockData.iat[0,164]=combo.iat[0,34]
            myStockData.iat[0,134]=combo.iat[0,35]
            myStockData.iat[0,165]=combo.iat[0,35]
            myStockData.iat[0,136]=combo.iat[0,36]
            myStockData.iat[0,137]=combo.iat[0,37]
            myStockData.iat[0,138]=combo.iat[0,38]
            myStockData.iat[0,139]=combo.iat[0,39]
            myStockData.iat[0,140]=combo.iat[0,40]
            myStockData.iat[0,141]=combo.iat[0,41]
            myStockData.iat[0,143]=combo.iat[0,42]
            myStockData.iat[0,144]=combo.iat[0,43]
            myStockData.iat[0,145]=combo.iat[0,44]
            myStockData.iat[0,173]=combo.iat[0,44]
            myStockData.iat[0,146]=combo.iat[0,45]
            myStockData.iat[0,147]=combo.iat[0,46]
            myStockData.iat[0,148]=combo.iat[0,47]
            myStockData.iat[0,149]=combo.iat[0,48]
            myStockData.iat[0,150]=combo.iat[0,49]
            myStockData.iat[0,151]=combo.iat[0,50]
            myStockData.iat[0,152]=combo.iat[0,51]
            myStockData.iat[0,190]=combo.iat[0,51]
            myStockData.iat[0,153]=combo.iat[0,52]
            myStockData.iat[0,154]=combo.iat[0,53]
            myStockData.iat[0,161]=combo.iat[0,55]
            myStockData.iat[0,162]=combo.iat[0,56]
            myStockData.iat[0,166]=combo.iat[0,57]
            myStockData.iat[0,167]=combo.iat[0,58]
            myStockData.iat[0,168]=combo.iat[0,59]
            myStockData.iat[0,169]=combo.iat[0,60]
            myStockData.iat[0,157]=combo.iat[0,61]
            myStockData.iat[0,170]=combo.iat[0,61]
            myStockData.iat[0,160]=combo.iat[0,62]
            myStockData.iat[0,171]=combo.iat[0,62]
            myStockData.iat[0,174]=combo.iat[0,63]
            myStockData.iat[0,175]=combo.iat[0,64]
            myStockData.iat[0,176]=combo.iat[0,65]
            myStockData.iat[0,177]=combo.iat[0,66]
            myStockData.iat[0,178]=combo.iat[0,67]
            myStockData.iat[0,179]=combo.iat[0,68]
            myStockData.iat[0,180]=combo.iat[0,69]
            myStockData.iat[0,181]=combo.iat[0,70]
            myStockData.iat[0,182]=combo.iat[0,71]
            myStockData.iat[0,183]=combo.iat[0,72]
            myStockData.iat[0,184]=combo.iat[0,73]
            myStockData.iat[0,185]=combo.iat[0,74]
            myStockData.iat[0,186]=combo.iat[0,75]
            myStockData.iat[0,189]=combo.iat[0,76]
            myStockData.iat[0,192]=combo.iat[0,78]
            myStockData.iat[0,194]=combo.iat[0,80]
            myStockData.iat[0,195]=combo.iat[0,81]
            myStockData.iat[0,196]=combo.iat[0,82]
            myStockData.iat[0,197]=combo.iat[0,83]
            myStockData.iat[0,198]=combo.iat[0,84]
            myStockData.iat[0,199]=combo.iat[0,85]
            myStockData.iat[0,200]=combo.iat[0,86]
            myStockData.iat[0,201]=combo.iat[0,87]
            myStockData.iat[0,202]=combo.iat[0,88]
            myStockData.iat[0,203]=combo.iat[0,89]
            myStockData.iat[0,204]=combo.iat[0,90]
            myStockData.iat[0,205]=combo.iat[0,91]
            myStockData.iat[0,206]=combo.iat[0,92]
            myStockData.iat[0,207]=combo.iat[0,93]
            myStockData.iat[0,208]=combo.iat[0,94]
            myStockData.iat[0,209]=combo.iat[0,95]
            myStockData.iat[0,210]=combo.iat[0,96]

            myStockData.iat[0,211]=combo.iat[0,97]
            myStockData.iat[0,212]=combo.iat[0,98]
            myStockData.iat[0,216]=combo.iat[0,99]
            myStockData.iat[0,217]=combo.iat[0,100]

            hider = False
    except KeyError:
        myStockData = pd.DataFrame()
        print("Key Error")
    except ValueError:
        print("Value Error")
        myStockData = pd.DataFrame()
    return myStockData

def runme():
    apikey = #YOUR API KEY HERE
    dataPath = #Path where you've saved your historical metrics
    spPath = #Path where you've saved your SP500 data
    savePath = #Path where you want to save your current data
    parse_data(apikey,dataPath,spPath,savePath)

if __name__ == "__main__":
    runme()
    