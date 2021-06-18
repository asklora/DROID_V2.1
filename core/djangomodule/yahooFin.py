import pandas as pd 
import requests
import json
import ast
import cfscrape
from core.djangomodule.network.cloud import DroidDb
from core.universe.models import Universe
from core.master.models import Currency,LatestPrice
from bs4 import BeautifulSoup as soup
from urllib.request import urlopen as uReq
from datetime import datetime

def get_quote_yahoo(ticker, use_symbol=False):
    api_key = "48c15ceb22mshe6bb12d6f379d74p146379jsnffadab4cee19"
    url = "https://yahoo-finance-low-latency.p.rapidapi.com/v6/finance/quote"
    header = {
		'x-rapidapi-key': api_key,
		'x-rapidapi-host': "yahoo-finance-low-latency.p.rapidapi.com"
	}
    # buat list sementara aq off in.. urgent
    
    

    # if isinstance(ticker, list):
	# 	if use_symbol:
	# 		ticker_ric = []
	# 		for tick in ticker:
	# 			try:
	# 				ric = Universe.objects.get(ticker_symbol=tick)
	# 				if ric.currency_code.currency_code != 'USD':
	# 					ticker_ric.append(ric.ticker)
	# 				else:
	# 					ticker_ric.append(tick)
	# 			except Universe.DoesNotExist:
	# 				pass
	# 		ticker = ticker_ric
	# 	if ticker != []:
	# 		ticker = ','.join([str(elem) for elem in ticker])
	# 	else:
	# 		data = {
	# 				"ticker":[],
	# 				"ask":[],
	# 				"bid":[]
	# 			}
	# 		return pd.DataFrame(data)
	# else:
	# 	try:
	# 		ric = Universe.objects.get(ticker_symbol=ticker)
	# 		if ric.currency_code.currency_code == 'USD':
	# 			ticker = ric.ticker_symbol
	# 		else:
	# 			if use_symbol:
	# 				ticker = ric.ticker
	# 			else:
	# 				ticker = ticker
	# 	except Universe.DoesNotExist:
	# 		data = {
	# 				"ticker":[],
	# 				"ask":[],
	# 				"bid":[]
	# 			}
	# 		return pd.DataFrame(data)
    if use_symbol:
        ric = Universe.objects.get(ticker=ticker)
        identifier = ric.ticker_symbol
    else:
        ric= Universe.objects.get(ticker=ticker)
        identifier = ric.ticker
    symbol = {
		"symbols" : identifier
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
        ### CARA SAVE DJANGO ONE BY ONE ###
        if use_symbol:
            ticker = Universe.objects.get(ticker_symbol=resp['symbol'])
            ric = LatestPrice.objects.get(ticker=ticker)
        else:
            ric = LatestPrice.objects.get(ticker=resp['symbol'])

        print(ric.ticker)
        ric.intraday_ask =resp['ask']
        ric.intraday_bid =resp['bid']
        ric.close =resp['regularMarketPrice']
        ric.last_date = datetime.now().date()
        ric.save()
        ### END SAVE DJANGO ONE BY ONE ###
        data['ticker'].append(resp['symbol'])
        data['bid'].append(resp['bid'])
        data['ask'].append(resp['ask'])
    df = pd.DataFrame(data)
    print(df)
    return df

def scrap_csi():
    price_float = None
    url = "http://www.csindex.com.cn/en/homeApply"
    # client = uReq(url)
    # html_page = client.read()
    # print(f"html == {html_page}")
    # close_client = client.close()
    scraper = cfscrape.create_scraper()
    scrap = scraper.get(url)
    html_page = scrap.content.decode('utf-8')
    page_soup = soup(html_page, "html.parser")
    owls = page_soup.find_all("div", {"class":"item"})
    for owl in owls:
        head = owl.find("h2",{"class":"g_name"})
        head_name = head.text.replace("\n","")
        if head_name == 'CSI 300':
            price = owl.find('span', {'class':'g_num'})
            price_float = float(price.text)
    return price_float

def get_quote_index(currency):
    api_key = "48c15ceb22mshe6bb12d6f379d74p146379jsnffadab4cee19"
    url = "https://yahoo-finance-low-latency.p.rapidapi.com/v6/finance/quote"
    header = {
		'x-rapidapi-key': api_key,
		'x-rapidapi-host': "yahoo-finance-low-latency.p.rapidapi.com"
	}
    curr = Currency.objects.get(currency_code=currency) # IF CNY
    if currency == "CNY":
        # price = scrap_csi()
        # curr.index_price = price
        # curr.save()
        pass
    else:
        identifier = curr.index_ticker.replace('.','^')
        symbol = {
    		"symbols" : identifier
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
            curr.index_price =resp['regularMarketPrice'] # USE FROM THE SCRAPPING ONE
            curr.save()



# if __name__ == "__main__":
# scrap_csi()
# theList = "AAPL.O,FB,AMZN,TSLA"
# add = ["002271.SZ","002304.SZ","601066.SS","601088.SS","005930.KS","039490.KS","042660.KS"]
# for data in add:
# 	theList += ","+data
# yahoo(theList)