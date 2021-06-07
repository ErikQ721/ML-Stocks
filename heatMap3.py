import pandas as pd
import numpy as np
import os
from combineMetricsData3 import combineData
from datetime import date

#Final step: once you've run through and generated all of the predicted stocks, parses through the data and finds the most called. 
#Weights for a larger amount of overperformance


def heatMap(folderPath,currentData):
    print("generating heat map")
    smallPath = folderPath + "\\PredictedStocks"
    myFiles = os.listdir(smallPath)
    heatMap = pd.DataFrame()
    count = 0

    for filename in myFiles:
        count = count+1

        multi = filename.replace("predictedStocks_",'')
        multi = int(multi[:-6])
        multi = (multi*0.01)
        multi_str = str(multi) + '_' +str(count)
        


        tickPath = smallPath+'\\'+filename
        theseStocks = pd.read_csv(tickPath,index_col='0')
        theseStocks.drop(theseStocks.columns[[0]],axis = 1,inplace = True)

        theseStocks[multi_str] = multi

        heatMap = pd.concat([heatMap,theseStocks],axis=1)
    
    myScores = heatMap.sum(axis = 1)
    heatMap.insert(0,'total score',myScores)
    heatMap.sort_values(by=['total score'],ascending = False, inplace = True)
    heatMap.drop(heatMap.iloc[:,1:],inplace = True,axis = 1)



    heatMap[str(date.today())] = np.nan

    for k in range(0,len(heatMap.index)):
        stock = heatMap.index[k]
        myStockData = currentData.loc[currentData['symbol']==stock]
        price = myStockData.iat[0,1]
        heatMap.iloc[k,-1] = price


    heatMap.rename(columns = {'0':'Calls'},inplace = True)
    heatMap.to_csv(folderPath+'\\HeatMap.csv')
    
    print("Program Finished")



def runner():
    folderPath = "E:\\PythonCode\\Data\\20210606_Data"
    currentDataPath = folderPath + "\\currentData"

    currentData = combineData(currentDataPath)
    heatMap(folderPath,currentData)

if __name__ == "__main__":
    runner()