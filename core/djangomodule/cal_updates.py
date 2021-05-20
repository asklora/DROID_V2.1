import requests
import json
from collections import OrderedDict

# USD, HKD, CNY, KRW, EUR, GBP, TWD, JPY, SGD
fin_id = "CN.SSE,US.NASDAQ,US.NYSE,HK.HKEX,KR.KRX,ES.BME,IT.MIB,DE.XETR,GB.LSE,TW.TWSE,JP.JPX"

tradinghours_token = "1M1a35Qhk8gUbCsOSl6XRY2z3Qjj0of7y5ZEfE5MasUYm5b9YsoooA7RSxW7"

########################### FROM GLOBAL_VARS.PY ###################################

DSS_USERNAME="9023786"

DSS_PASSWORD="askLORA20$"

URL_Extrations="https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/Extract"

URL_AuthToken="https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken"

REPORT_INTRADAY="Intraday_Pricing"

####################################################################################


def get_index_symbol():
    url = "https://api.tradinghours.com/v3/markets?group=core&token="+tradinghours_token
    req = requests.get(url)
    res = req.json()
    print(res)
    return True

def get_trading_hour():
    url = "http://api.tradinghours.com/v3/markets/status?fin_id="+fin_id+"&token="+tradinghours_token
    req = requests.get(url)
    res = req.json()
    # print(res)
    list_index = list(fin_id.split(","))
    # print(len(list_index))
    # for data in res['data'].keys():
    #   print(data)
    for data in list_index:
        print(data, ": ", res['data'][data]['status'])
        print("===========================================")
    return True

def makeExtractHeader(token):
    _header = {}
    _header['Prefer'] = 'respond-async, wait=5'
    _header['Content-Type'] = 'application/json; odata.metadata=minimal'
    _header['Accept-Charset'] = 'UTF-8'
    _header['Authorization'] = token
    return _header


def get_quote_date_dss():
    # authToken = None
    # header_get_token = {
    #     "Content-Type": "application/json; odata.metadata=minimal",
    #     "Prefer": "respond-async"
    # }

    # req_get_token = {
    #     'Credentials': {
    #         'Password': DSS_PASSWORD, 
    #         'Username': DSS_USERNAME
    #     }
    # }

    # req_get_token = json.dumps(req_get_token)

    # req = requests.post(URL_AuthToken, json=req_get_token, headers=header_get_token)
    # if req.status_code == 200:
    #     resp = req.json()
    #     authToken = resp['value']
    # else:
    #     return False

    # if authToken != None:
        # token = "Token "+authToken
        # header = makeExtractHeader(token)
    data = {
        "ExtractionRequest": {
            "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.IntradayPricingExtractionRequest",
            "ContentFieldNames": [
                "RIC",
                "Trade Date"
            ],
            "IdentifierList": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
                "InstrumentIdentifiers": [
                ],
                "ValidationOptions":{"AllowHistoricalInstruments":True,"AllowOpenAccessInstruments":True},
                "UseUserPreferencesForValidationOptions": False
            },
            "Condition":
            {
                
          }
        }
    }
    data['ExtractionRequest']['Condition'] = OrderedDict(data['ExtractionRequest']['Condition'])
    data['ExtractionRequest']['IdentifierList']['ValidationOptions'] = OrderedDict(data['ExtractionRequest']['IdentifierList']['ValidationOptions'])
    data['ExtractionRequest']['IdentifierList'] = OrderedDict(data['ExtractionRequest']['IdentifierList'])
    data['ExtractionRequest'] = OrderedDict(data['ExtractionRequest'])
    data = OrderedDict(data)
    # data = json.dumps(data)
    # data = json.load(data, object_pairs_hook=OrderedDict)

    listofticker = [".KS200", ".SPX", ".HSLI", ".CSI300"]


    # stocks = universe.objects.filter(is_active=True, ticker=listofticker)
    for _inst in listofticker:
        _inst = "/"+_inst
        data["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append(
                {"IdentifierType": "Ric", "Identifier": _inst})
    print(data)
    return data

# import pandas as pd
# from general.sql_query import get_active_universe
# from general.date_process import datetimeNow
# from global_vars import REPORT_INTRADAY, REPORT_HISTORY
# from datasource.dss import get_data_from_dss

# if __name__ == '__main__':
#     print("Do Process")
#     print("{} : === Currency Price Ingestion ===".format(datetimeNow()))
#     currencylist = get_active_universe(ticker=[".KS200", ".SPX", ".HSLI", ".CSI300"])
#     # "select * from universe where is_active=True and ticker in (select ticker from universe where is_active=True and ticker in ('.KS200', '.SPX', '.HSLI', '.CSI300')) order by ticker"
#     currencylist = pd.DataFrame({"ticker" : [".KS200", ".SPX", ".HSLI", ".CSI300"]}, index=[0, 1, 2, 3])
#     jsonFileName = "files/file_json/intraday_capital_change.json"
#     currencylist["ticker"] = "/" + currencylist["ticker"]
#     # "/.KS200", "/.SPX", "/.HSLI", "/.CSI300"
#     print(currencylist)
#     result = get_data_from_dss("start_date", "end_date", currencylist["ticker"], jsonFileName, report=REPORT_INTRADAY)
#     print(result)
#     # result = result.drop(columns=["IdentifierType", "Identifier"])
#     print(result)

get_quote_date_dss()

