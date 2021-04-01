
from datetime import date,timedelta
import pandas as pd
import time
import numpy as np
from tqdm import tqdm
import fmpsdk

#Third Step, gathers the most recent market data for each stock


def preprocess_price_data():
    #Gets the price data from the previous CSVs and cleans them by eliminating weekends
    sp_dat = pd.read_csv("E:\\PythonCode\\Data\\20210216_Data\\sp500_index.csv", index_col="date", parse_dates=True)
    stock_dat = pd.read_csv("E:\\PythonCode\\Data\\20210205_Data\\stock_prices_All_Long_Clean.csv", index_col="date", parse_dates=True)

    start_date = sp_dat.index[-1]
    end_date = sp_dat.index[0]
    idx = pd.date_range(start_date, end_date, freq="D")

    sp500_raw_data = sp_dat.reindex(idx)
    stock_raw_data = stock_dat.reindex(idx)

    sp500_raw_data.ffill(inplace=True)
    stock_raw_data.ffill(inplace=True)

    return sp500_raw_data, stock_raw_data

def percent_change(current_date,tick_dat,stk_price):
    year_ago = current_date - timedelta(days = 366)
    year_ago = pd.to_datetime(year_ago)
    price_now_stk = stk_price

    try:
        price_then_stk = tick_dat.loc[year_ago]
    except KeyError:
        price_then_stk = tick_dat.loc[year_ago]

    stock_p_change = (price_now_stk - price_then_stk)*100/price_then_stk
    return stock_p_change

def sp_percent_change(current_date,sp_dat,sp_price):
    year_ago = current_date - timedelta(days = 366)
    year_ago = pd.to_datetime(year_ago)
    price_now_sp = sp_price
    try:
        price_then_sp = sp_dat.loc[year_ago,'adjClose']
    except KeyError:
        price_then_sp = sp_dat.loc[year_ago,'adjClose']
    
    sp_p_change = (price_now_sp - price_then_sp)*100/price_then_sp
    return sp_p_change
    
def get_current_data(apikey, myPath):
    #Goes through the historical database, looks to see if the stock data has been updated in the last quarter. If so, sends stock it to getLastDat()
    #Data gets updated in getLastDat() and saved to a dataframe with the most updated values
    sp_hist, stock_hist = preprocess_price_data()
    spy_bigDat = pd.DataFrame.from_dict(fmpsdk.historical_price_full(apikey=apikey, symbol='SPY'))
    historical = pd.DataFrame(spy_bigDat['historical'].tolist())
    historical.set_index('date',inplace = True)
    SPY_price_df = historical['adjClose']
    SPY_price = SPY_price_df.head(1)

    day_now = date.today()
    sp_p_change = sp_percent_change(day_now,sp_hist,SPY_price)

    old_data = pd.read_csv('E:\\PythonCode\\Data\\20210228_Data\\stock_metrics.csv',index_col='date')
    tick_names = old_data['symbol'].drop_duplicates()

    full_data = pd.DataFrame()

    for mySTK in tqdm(tick_names, desc="Parsing progress:", unit="tickers"):
        print("")
        print(mySTK)
        
        tick_dat = stock_hist[mySTK]
        
        stk_last_dat = pd.DataFrame()
        my_stk_data = old_data.loc[old_data['symbol']==mySTK]
        stk_last_dat = my_stk_data.head(1)
        index = my_stk_data.index
        index_time = pd.to_datetime(index[0])
        start_date = pd.to_datetime('12/15/2020')

        if (index_time>start_date):
            try:
                stk_last_dat = getLastDat(mySTK, apikey,tick_dat,SPY_price, sp_p_change,stk_last_dat)
            
                full_data = pd.concat([full_data,stk_last_dat],axis = 0, sort=False)
            except ValueError:
                print('val error occurred')
            except KeyError:
                print('key error occurred')
    
    fullPath = myPath + '\\current_data_SD20201215.csv'
    full_data.to_csv(fullPath)


def getLastDat(mySTK, apikey,tick_dat,SPY_price, sp_p_change,last_dat):
    day_now = date.today()
    stk_priceDat = pd.DataFrame.from_dict(fmpsdk.historical_price_full(apikey=apikey, symbol=mySTK))
    stk_hist = pd.DataFrame(stk_priceDat['historical'].tolist())
    stk_hist.set_index('date',inplace = True)
    stk_price_df = stk_hist['adjClose']
    mySTK_price = stk_price_df.head(1)

    stock_p_change = percent_change(day_now,tick_dat,mySTK_price)

    fin_ratTTM = pd.DataFrame.from_dict(fmpsdk.financial_ratios_ttm(apikey=apikey, symbol=mySTK))
    key_metTTM = pd.DataFrame.from_dict(fmpsdk.key_metrics_ttm(apikey=apikey, symbol=mySTK))

    fin_ratTTM.fillna(0,inplace = True)
    key_metTTM.fillna(0,inplace = True)
    divYield = fin_ratTTM['dividendYielTTM']
            
    PER = fin_ratTTM['peRatioTTM']
    PEG = fin_ratTTM['pegRatioTTM']
    PayRat = fin_ratTTM['payoutRatioTTM']
    CurrentRat = fin_ratTTM['currentRatioTTM']
    qRat = fin_ratTTM['quickRatioTTM']
    cashRat = fin_ratTTM['cashRatioTTM']
    DOSO = fin_ratTTM['daysOfSalesOutstandingTTM']
    DOIO = fin_ratTTM['daysOfInventoryOutstandingTTM']
    opCY = fin_ratTTM['operatingCycleTTM']
    DOPO = fin_ratTTM['daysOfPayablesOutstandingTTM']
    cashCon = fin_ratTTM['cashConversionCycleTTM']
    gPM = fin_ratTTM['grossProfitMarginTTM']
    oPM = fin_ratTTM['operatingProfitMarginTTM']
    prePM = fin_ratTTM['pretaxProfitMarginTTM']
    nPM = fin_ratTTM['netProfitMarginTTM']
    effTR = fin_ratTTM['effectiveTaxRateTTM']
    rOA = fin_ratTTM['returnOnAssetsTTM']
    rOE = fin_ratTTM['returnOnEquityTTM']
    rOCE = fin_ratTTM['returnOnCapitalEmployedTTM']
    nIPEBT = fin_ratTTM['netIncomePerEBTTTM']
    ebtPEbit = fin_ratTTM['ebtPerEbitTTM']
    ebitPR = fin_ratTTM['ebitPerRevenueTTM']
    dRat = fin_ratTTM['debtRatioTTM']
    dEqRat = fin_ratTTM['debtEquityRatioTTM']
    lTDTC = fin_ratTTM['longTermDebtToCapitalizationTTM']
    tDTC = fin_ratTTM['totalDebtToCapitalizationTTM']
    intCov = fin_ratTTM['interestCoverageTTM']
    cFtD = fin_ratTTM['cashFlowToDebtRatioTTM']
    compEqX = fin_ratTTM['companyEquityMultiplierTTM']
    rec_turn = fin_ratTTM['receivablesTurnoverTTM']
    payTurn = fin_ratTTM['payablesTurnoverTTM']
    invTurn = fin_ratTTM['inventoryTurnoverTTM']
    fixedAss = fin_ratTTM['fixedAssetTurnoverTTM']
    assTurn = fin_ratTTM['assetTurnoverTTM']
    oCFPS = fin_ratTTM['operatingCashFlowPerShareTTM']
    fCFPS = fin_ratTTM['freeCashFlowPerShareTTM']
    cPS = fin_ratTTM['cashPerShareTTM']
    oCFSR = fin_ratTTM['operatingCashFlowSalesRatioTTM']
    fCFOCFR = fin_ratTTM['freeCashFlowOperatingCashFlowRatioTTM']
    cFCR = fin_ratTTM['cashFlowCoverageRatiosTTM']
    sTCR = fin_ratTTM['shortTermCoverageRatiosTTM']
    cECR = fin_ratTTM['capitalExpenditureCoverageRatioTTM']
    dPaCCR= fin_ratTTM['dividendPaidAndCapexCoverageRatioTTM']
    pBVR = fin_ratTTM['priceBookValueRatioTTM']
    ptBR = fin_ratTTM['priceToBookRatioTTM']
    ptSR = fin_ratTTM['priceToSalesRatioTTM']
    ptFCFR = fin_ratTTM['priceToFreeCashFlowsRatioTTM']
    ptOCFR = fin_ratTTM['priceToOperatingCashFlowsRatioTTM']
    pCFR = fin_ratTTM['priceCashFlowRatioTTM']
    pSR = fin_ratTTM['priceSalesRatioTTM']
    eVX = fin_ratTTM['enterpriseValueMultipleTTM']
    pFV = fin_ratTTM['priceFairValueTTM']


    rPS = key_metTTM['revenuePerShareTTM']
    nIPS = key_metTTM['netIncomePerShareTTM']
    bVPS = key_metTTM['bookValuePerShareTTM']
    tBVPS = key_metTTM['tangibleBookValuePerShareTTM']
    sEPS = key_metTTM['shareholdersEquityPerShareTTM']
    iDPS = key_metTTM['interestDebtPerShareTTM']
    marCap = key_metTTM['marketCapTTM']
    entVal = key_metTTM['enterpriseValueTTM']
    evtS = key_metTTM['evToSalesTTM']
    evOEBITDA = key_metTTM['enterpriseValueOverEBITDATTM']
    evtOCF = key_metTTM['evToOperatingCashFlowTTM']
    evtFCF = key_metTTM['evToFreeCashFlowTTM']
    eY = key_metTTM['earningsYieldTTM']
    fCFY = key_metTTM['freeCashFlowYieldTTM']
    dtE = key_metTTM['debtToEquityTTM']
    dtA = key_metTTM['debtToAssetsTTM']
    nDtEBITDA = key_metTTM['netDebtToEBITDATTM']
    iQ = key_metTTM['incomeQualityTTM']
    sGaAtR = key_metTTM['salesGeneralAndAdministrativeToRevenueTTM']
    raDtR = key_metTTM['researchAndDevelopementToRevenueTTM']
    itTA = key_metTTM['intangiblesToTotalAssetsTTM']
    ctOCF = key_metTTM['capexToOperatingCashFlowTTM']
    ctR = key_metTTM['capexToRevenueTTM']
    ctD = key_metTTM['capexToDepreciationTTM']
    sBCtR = key_metTTM['stockBasedCompensationToRevenueTTM']
    gNum = key_metTTM['grahamNumberTTM']
    roic = key_metTTM['roicTTM']
    rOTA = key_metTTM['returnOnTangibleAssetsTTM']
    gNN = key_metTTM['grahamNetNetTTM']
    wC = key_metTTM['workingCapitalTTM']
    tAV = key_metTTM['tangibleAssetValueTTM']
    nCAV = key_metTTM['netCurrentAssetValueTTM']
    invCap = key_metTTM['investedCapitalTTM']
    avgRec = key_metTTM['averageReceivablesTTM']
    avgPay = key_metTTM['averagePayablesTTM']
    avgInv = key_metTTM['averageInventoryTTM']
    dSO = key_metTTM['daysSalesOutstandingTTM']
    dPO = key_metTTM['daysPayablesOutstandingTTM']
    dOIOH = key_metTTM['daysOfInventoryOnHandTTM']
    capPS = key_metTTM['capexPerShareTTM']
    pbRat = key_metTTM['pbRatioTTM']
    ptbRatio = key_metTTM['ptbRatioTTM']



    index = [day_now] + last_dat.index.tolist()[1:]
    last_dat.index = index

    last_dat.iat[0,1] = mySTK_price
    last_dat.iat[0,2] = stock_p_change
    last_dat.iat[0,3] = SPY_price
    last_dat.iat[0,4] = sp_p_change
    
    
    
    
    last_dat.iat[0,98] = CurrentRat
    last_dat.iat[0,99] = qRat
    last_dat.iat[0,100] = cashRat
    last_dat.iat[0,101] = DOSO
    last_dat.iat[0,102] = DOIO
    last_dat.iat[0,103] = opCY
    last_dat.iat[0,104] = DOPO
    last_dat.iat[0,105] = cashCon
    last_dat.iat[0,106] = gPM
    last_dat.iat[0,107] = oPM
    last_dat.iat[0,108] = prePM
    last_dat.iat[0,109] = nPM
    last_dat.iat[0,110] = effTR
    last_dat.iat[0,111] = rOA
    last_dat.iat[0,112] = rOE
    last_dat.iat[0,113] = rOCE
    last_dat.iat[0,114] = nIPEBT
    last_dat.iat[0,115] = ebtPEbit
    last_dat.iat[0,116] = ebitPR
    last_dat.iat[0,117] = dRat
    last_dat.iat[0,118] = dEqRat
    last_dat.iat[0,119] = lTDTC
    last_dat.iat[0,120] = tDTC
    last_dat.iat[0,121] = intCov
    last_dat.iat[0,122] = cFtD
    last_dat.iat[0,123] = compEqX
    last_dat.iat[0,124] = rec_turn
    last_dat.iat[0,125] = payTurn
    last_dat.iat[0,126] = invTurn
    last_dat.iat[0,127] = fixedAss
    last_dat.iat[0,128] = assTurn
    last_dat.iat[0,129] = oCFPS
    last_dat.iat[0,130] = fCFPS
    last_dat.iat[0,131] = cPS
    last_dat.iat[0,132] = PayRat
    last_dat.iat[0,133] = oCFSR
    last_dat.iat[0,134] = fCFOCFR
    last_dat.iat[0,135] = cFCR
    last_dat.iat[0,136] = sTCR            
    last_dat.iat[0,137] = cECR
    last_dat.iat[0,138] = dPaCCR
    last_dat.iat[0,140] = pBVR
    last_dat.iat[0,141] = ptBR
    last_dat.iat[0,142] = ptSR
    last_dat.iat[0,143] = PER
    last_dat.iat[0,144] = ptFCFR
    last_dat.iat[0,145] = ptOCFR
    last_dat.iat[0,146] = pCFR
    last_dat.iat[0,147] = PEG
    last_dat.iat[0,148] = ptSR
    last_dat.iat[0,149] = divYield
    last_dat.iat[0,150] = eVX
    last_dat.iat[0,151] = pFV
    last_dat.iat[0,155] = entVal
    last_dat.iat[0,156] = rPS
    last_dat.iat[0,157] = nIPS
    last_dat.iat[0,158] = bVPS
    last_dat.iat[0,159] = tBVPS
    last_dat.iat[0,160] = sEPS
    last_dat.iat[0,161] = iDPS
    last_dat.iat[0,162] = marCap
    last_dat.iat[0,163] = PER
    last_dat.iat[0,164] = ptOCFR
    last_dat.iat[0,165] = ptFCFR
    last_dat.iat[0,166] = pbRat
    last_dat.iat[0,167] = ptbRatio
    last_dat.iat[0,168] = evtS
    last_dat.iat[0,169] = evOEBITDA
    last_dat.iat[0,170] = evtOCF
    last_dat.iat[0,171] = evtFCF
    last_dat.iat[0,172] = eY
    last_dat.iat[0,173] = fCFY
    last_dat.iat[0,174] = dtE
    last_dat.iat[0,175] = dtA
    last_dat.iat[0,176] = nDtEBITDA
    last_dat.iat[0,177] = iQ
    last_dat.iat[0,178] = sGaAtR
    last_dat.iat[0,179] = raDtR
    last_dat.iat[0,180] = itTA
    last_dat.iat[0,181] = ctOCF
    last_dat.iat[0,182] = ctR
    last_dat.iat[0,183] = ctD
    last_dat.iat[0,184] = sBCtR
    last_dat.iat[0,185] = gNum
    last_dat.iat[0,186] = roic
    last_dat.iat[0,187] = rOTA
    last_dat.iat[0,188] = gNN
    last_dat.iat[0,189] = wC
    last_dat.iat[0,190] = tAV
    last_dat.iat[0,191] = nCAV
    last_dat.iat[0,192] = invCap
    last_dat.iat[0,193] = avgRec
    last_dat.iat[0,194] = avgPay
    last_dat.iat[0,195] = avgInv
    last_dat.iat[0,196] = dSO
    last_dat.iat[0,197] = dPO
    last_dat.iat[0,198] = dOIOH
    last_dat.iat[0,199] = rOE
    last_dat.iat[0,200] = capPS
    return last_dat


if __name__ == "__main__":
    apikey = '*YOUR FMP API KEY HERE*'
    curPath = "E:\\PythonCode\\Data\\20210314_Data"
    get_current_data(apikey, curPath)