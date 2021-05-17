import pandas as pd 
import requests
import json
import ast
from core.djangomodule.network.cloud import DroidDb
# from core.universe.models import Universe

def yahoo(ticker):
	api_key = "48c15ceb22mshe6bb12d6f379d74p146379jsnffadab4cee19"
	url = "https://yahoo-finance-low-latency.p.rapidapi.com/v6/finance/quote"
	header = {
		'x-rapidapi-key': api_key,
		'x-rapidapi-host': "yahoo-finance-low-latency.p.rapidapi.com"
	}
	if isinstance(ticker, list):
		ticker = ','.join([str(elem) for elem in ticker])

	symbol = {
		"symbols" : ticker
	}
	# print(symbol)
	req = requests.get(url, headers=header, params=symbol)
	res = req.json()
	res = res['quoteResponse']['result']
	# last = []
	data = {
		"ticker":[],
		"ask":[],
		"bid":[]
	}
	for resp in res:
		data['ticker'].append(resp['symbol'])
		data['bid'].append(resp['bid'])
		data['ask'].append(resp['ask'])
	df = pd.DataFrame(data)
	print(df)
	return df

# # if __name__ == "__main__":
# theList = "AAPL.O,FB,AMZN,TSLA"
# add = ["002271.SZ","002304.SZ","601066.SS","601088.SS","005930.KS","039490.KS","042660.KS"]
# for data in add:
# 	theList += ","+data
# yahoo(theList)