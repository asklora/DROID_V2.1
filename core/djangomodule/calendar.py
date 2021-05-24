import requests
import json
import ast
from datetime import datetime, timedelta
from collections import OrderedDict
from core.universe.models import ExchangeMarket
# USD, HKD, CNY, KRW, EUR, GBP, TWD, JPY, SGD
fin_id = "CN.SSE,US.NASDAQ,US.NYSE,HK.HKEX,KR.KRX,ES.BME,IT.MIB,DE.XETR,GB.LSE,TW.TWSE,JP.JPX"

tradinghours_token = "1M1a35Qhk8gUbCsOSl6XRY2z3Qjj0of7y5ZEfE5MasUYm5b9YsoooA7RSxW7"

########################### FROM GLOBAL_VARS.PY ###################################

DSS_USERNAME = "9023786"

DSS_PASSWORD = "askLORA20$"

URL_Extrations = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/Extract"

URL_AuthToken = "https://hosted.datascopeapi.reuters.com/RestApi/v1/Authentication/RequestToken"

REPORT_INTRADAY = "Intraday_Pricing"

####################################################################################


class TradingHours:

    def __init__(self, fins=None,mic=None):
        if mic:
            if isinstance(mic,str):
                exchange = ExchangeMarket.objects.get(mic=mic)
                self.fin_id =exchange.fin_id
            elif isinstance(mic,list):
                exchange = ExchangeMarket.objects.filter(mic__in=mic)
                self.fin_id =[finid.fin_id for finid in exchange]
            else:
                mictype=type(mic)
                raise ValueError(f"mic must be string or list instance not {mictype}")
        elif fins:
            self.fin_id = fins
        elif not fins and not mic:
            raise ValueError("fins and mic should not be Blank")
        self.token = tradinghours_token

    def get_index_symbol(self):
        url = "https://api.tradinghours.com/v3/markets?group=core&token="+self.token
        req = requests.get(url)
        res = req.json()
        return res

    def get_weekly_holiday(self):
        print(self.fin_id)
        if type(self.fin_id) == list:
            fin_id = (",").join(self.fin_id)
        else:
            fin_id = self.fin_id
        start_date = datetime.now()
        end_date = start_date+timedelta(days=5)
        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")
        url = "https://api.tradinghours.com/v3/markets/holidays?fin_id=" + \
            fin_id+"&start="+start_date+"&end="+end_date+"&token="+self.token
        req = requests.get(url)
        if req.status_code == 200:
            resp = req.json()
            #########################save it to currency calendar##################################
            #######################################################################################
        else:
            resp = req.text
        print(resp)
        return True
    
    
    @property
    def fin_id(self):
        return self._fin_id

    @fin_id.setter
    def fin_id(self, value):
        self._fin_id = value

    @property
    def is_open(self):
        status = None
        if isinstance(self.fin_id,list):
            fin_param = (",").join(self.fin_id)
        else:
            fin_param = self.fin_id
        url = "http://api.tradinghours.com/v3/markets/status?fin_id=" + \
            fin_param+"&token="+self.token
        req = requests.get(url)

        if req.status_code == 200:
            resp = req.json()
            if isinstance(self.fin_id,str):
                stat = resp['data'][self.fin_id]['status']
                if stat == "Closed":
                    return False
                else:
                    return True
            else:
                status = []
                for fin in self.fin_id:
                    stat = resp["data"][fin]['status']
                    if stat == "Closed":
                        data = {fin: False}
                    else:
                        data = {fin: True}
                    status.append(data)
        else:
            resp = req.text
            print(fin_param,'error')
            status = False
        return status

# def get_trading_hour(currencylist):
# 	fin_id = []
# 	for curr in currencylist:
# 		if comparation[curr] == {}:
# 			pass
# 		else:
# 			fin_id.append(comparation[curr]["fin_id"])
# 	if fin_id != []:
# 		fin_id = (",").join(fin_id)
# 	else:
# 		print("Currency does not available")
# 		return False
# 	url = "http://api.tradinghours.com/v3/markets/status?fin_id="+fin_id+"&token="+tradinghours_token
# 	req = requests.get(url)
# 	res = req.json()
# 	# print(res)
# 	list_index = list(fin_id.split(","))
# 	# print(len(list_index))
# 	# for data in res['data'].keys():
# 	#   print(data)
# 	for data in list_index:
# 		print(data, ": ", res['data'][data]['status'])
# 		print("===========================================")

# 	dss_date = get_quote_date_dss()
# 	if dss_date != False:
# 		if type(dss_date) == dict:
# 			for dss in dss_date['value']:
# 				print(dss)
# 				print("===========================================")
# 	return True

# def makeExtractHeader(token):
# 	_header = {}
# 	_header['Prefer'] = 'respond-async, wait=5'
# 	_header['Content-Type'] = 'application/json; odata.metadata=minimal'
# 	_header['Accept-Charset'] = 'UTF-8'
# 	_header['Authorization'] = token
# 	return _header


# def get_quote_date_dss():
# 	# print("=====================")
# 	authToken = None
# 	header_get_token = {
# 		"Content-Type": "application/json; odata.metadata=minimal",
# 		"Prefer": "respond-async"
# 	}

# 	req_get_token = {'Credentials': {'Password': DSS_PASSWORD, 'Username': DSS_USERNAME}}

# 	req_get_token = json.dumps(req_get_token)

# 	req = requests.post(URL_AuthToken, data=req_get_token, headers=header_get_token)
# 	# print("req_token_payload >> ", req_get_token)
# 	if req.status_code == 200:
# 		resp = req.json()
# 		authToken = resp['value']
# 	else:
# 		# print("token respo >> ", req.text)
# 		return False

# 	# print("authToken >> ", authToken)

# 	if authToken != None:
# 		token = "Token "+authToken
# 		header = makeExtractHeader(token)
# 	data = {
# 		"ExtractionRequest": {
# 			"@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.IntradayPricingExtractionRequest",
# 			"ContentFieldNames": [
# 				"RIC",
# 				"Trade Date"
# 			],
# 			"IdentifierList": {
# 				"@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
# 				"InstrumentIdentifiers": [
# 				],
# 				"ValidationOptions":{"AllowHistoricalInstruments":True,"AllowOpenAccessInstruments":True},
# 				"UseUserPreferencesForValidationOptions": False
# 			},
# 			"Condition":
# 			{

# 		  }
# 		}
# 	}
# 	data['ExtractionRequest']['Condition'] = OrderedDict(data['ExtractionRequest']['Condition'])
# 	data['ExtractionRequest']['IdentifierList']['ValidationOptions'] = OrderedDict(data['ExtractionRequest']['IdentifierList']['ValidationOptions'])
# 	data['ExtractionRequest']['IdentifierList'] = OrderedDict(data['ExtractionRequest']['IdentifierList'])
# 	data['ExtractionRequest'] = OrderedDict(data['ExtractionRequest'])
# 	data = OrderedDict(data)
# 	# data = json.dumps(data)
# 	# data = json.load(data, object_pairs_hook=OrderedDict)

# 	listofticker = [".KS200", ".SPX", ".HSLI", ".CSI300"]


# 	# stocks = universe.objects.filter(is_active=True, ticker=listofticker)
# 	for _inst in listofticker:
# 		_inst = "/"+_inst
# 		data["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append(
# 				{"IdentifierType": "Ric", "Identifier": _inst})
# 	dss_resp = requests.post(URL_Extrations, data=None, json=data, headers=header)
# 	print("status_code", dss_resp.status_code)
# 	print(dss_resp.text)
# 	if dss_resp.status_code == 200:
# 		return ast.literal_eval(dss_resp.text)
# 	else:
# 		print("False")
# 		return dss_resp.text

# if __name__ == '__main__':
# 	# get_trading_hour()
# 	x = tradingHours(["GB.LSE", "CN.SSE"])
# 	x.check_for_market()
