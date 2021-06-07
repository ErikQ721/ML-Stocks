import pandas as pd
import numpy as np
from tqdm import tqdm
import os

#finds your data and combines the csvs

def combineData(myPath):

    longPath = myPath

    dirList = os.listdir(longPath)
    final = pd.DataFrame()
    for name in tqdm(dirList,desc="combining", unit="files"):
        if name.endswith('.csv'):
            filePath = longPath+'\\'+name
            current = pd.read_csv(filePath, index_col = 'date')
            final = pd.concat([final,current],axis = 0)
    
    return final
    print("Finished Combining")

def runme():
    path = "E:\\PythonCode\\Data\\20210418_Data"
    savePath = path+'\\currentDataCombined.csv'
    myMets = combineData(path)
    myMets.to_csv(savePath)


if __name__ == "__main__":
    runme()