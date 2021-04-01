import pandas as pd
from sklearn.ensemble import RandomForestClassifier
import os
import current_data2
from tqdm import tqdm

def build_data_set(OUTPERFORMANCE = 100):
    """
    Reads the stock_metrics.csv file and prepares it for scikit-learn
    :return: X_train and y_train numpy arrays
    """
    training_data = pd.read_csv("E:\\PythonCode\\Data\\20210228_Data\\stock_metrics.csv", index_col="date")
    training_data.fillna(value= 0, axis=0, inplace=True)
    features = training_data.columns[6:]

    X_train = training_data[features].values
    # Generate the labels: '1' if a stock beats the S&P500 by more than 10%, else '0'.
    y_train = list(
        status_calc(
            training_data["stock_p_change"],
            training_data["SP500_p_change"],
            OUTPERFORMANCE,
        )
    )

    return X_train, y_train


def predict_stocks(OUTPERFORMANCE, currentDataPath, saveDataPath, counter):
    X_train, y_train = build_data_set(OUTPERFORMANCE)
    clf = RandomForestClassifier(n_estimators=100)
    clf.fit(X_train, y_train)
    #print(clf.score())

    # Now we get the actual data from which we want to generate predictions.
    data = pd.read_csv(currentDataPath)
    data.fillna(value= 0, axis=0, inplace=True)
    features = data.columns[7:]
    X_test = data[features].values
    z = data["symbol"].values

    # Get the predicted tickers
    y_pred = clf.predict(X_test)
    if sum(y_pred) == 0:
        print("No stocks predicted!")
    else:
        invest_list = z[y_pred].tolist()
        investlst = pd.DataFrame(invest_list)
        finalPath = f"predictedStocks_{OUTPERFORMANCE}_{counter}.csv"
        investlst.to_csv(saveDataPath + "\\" + finalPath)

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

def status_calc(stock, sp500, outperformance=10):
    """A simple function to classify whether a stock outperformed the S&P500
    :param stock: stock price
    :param sp500: S&P500 price
    :param outperformance: stock is classified 1 if stock price > S&P500 price + outperformance
    :return: true/false
    """
    if outperformance < 0:
        raise ValueError("outperformance must be positive")
    return stock - sp500 >= outperformance

def runner():
    apikey = '*YOUR API KEY HERE*'
    #looks for data path 
    bigPath = "E:\\PythonCode\\Data\\20210328_Data"
    smallPath = bigPath + "\\PredictedStocks_NotScaled"
    currentDataPath = "\\current_data_SD20201215.csv"
    #creates path if not there
    if not os.path.exists(smallPath):
        print("Creating folder")
        os.makedirs(smallPath)

    #If current data is not found, downloads the current data first
    if not os.path.isfile(bigPath+currentDataPath):
        print("Generating current data")
        current_data2.get_current_data(apikey, bigPath)
    
    #Gradually increases the outperformance metric, each time a stock is called, gets added to a new csv. Heat map looks at the tickers that have been called the most.
    print("running prediction loop")
    for i in range(11):
        myPerf = 15*(i)+50
        print(i)
        for j in range(10):
            print(j)
            predict_stocks(myPerf, bigPath+currentDataPath, smallPath, j+1)
    
    


    heatMap(smallPath)

def heatMap(smallPath):
    print("generating heat map")
    stksPred = pd.DataFrame()
    for filename in os.listdir(smallPath):
        tickPath = smallPath+'\\'+filename
        theseStocks = pd.read_csv(tickPath)
        frames = [stksPred,theseStocks]
        stksPred = pd.concat(frames)


        mySer = stksPred[stksPred.columns[1]].value_counts()
        mySer.to_csv(smallPath+'\\HeatMap.csv')
    
    print("Program Finished")

if __name__ == "__main__":
    runner()
