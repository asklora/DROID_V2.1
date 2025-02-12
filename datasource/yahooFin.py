import pandas as pd 
import requests
import cfscrape
import cfscrape
from core.djangomodule.network.cloud import DroidDb
from core.universe.models import Universe
from core.master.models import Currency,LatestPrice
from bs4 import BeautifulSoup as soup
from datetime import datetime
from core.services.models import ErrorLog


def get_quote_yahoo(ticker, use_symbol=False):
    api_key = "48c15ceb22mshe6bb12d6f379d74p146379jsnffadab4cee19"
    url = "https://yahoo-finance-low-latency.p.rapidapi.com/v6/finance/quote"
    header = {
		'x-rapidapi-key': api_key,
		'x-rapidapi-host': "yahoo-finance-low-latency.p.rapidapi.com"
	}

    try:
        ric = Universe.objects.get(ticker=ticker)
    except Universe.DoesNotExist as e:
        err = ErrorLog.objects.create_log(error_description=f'{ticker} not exist',error_message=str(e))
        err.send_report_error()
        return
    except Universe.MultipleObjectsReturned as e:
        err = ErrorLog.objects.create_log(error_description=f'error {ticker} return multiple',error_message=str(e))
        err.send_report_error()
        return
    if use_symbol:
        identifier = ric.ticker_symbol
    else:
        identifier = ric.ticker
    symbol = {
		"symbols" : identifier
	}
	# print(symbol)
    req = requests.get(url, headers=header, params=symbol)
    if req.status_code == 200:
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
                ticker = Universe.objects.get(ticker=ric.ticker)
                ric = LatestPrice.objects.get(ticker=ticker)
            else:
                ric = LatestPrice.objects.get(ticker=resp['symbol'])

            print(ric.ticker)
            ric.intraday_ask =resp['ask']
            ric.intraday_bid =resp['bid']
            ric.close =resp['regularMarketPrice']
            ric.latest_price =resp['regularMarketPrice']
            ric.last_date = datetime.now().date()
            ric.save()
            ### END SAVE DJANGO ONE BY ONE ###
            data['ticker'].append(ric.ticker)
            data['bid'].append(resp['bid'])
            data['ask'].append(resp['ask'])
            data['latest_price'].append(resp['regularMarketPrice'])
        df = pd.DataFrame(data)
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
        try:
            price = scrap_csi()
        except Exception as e:
            err = ErrorLog.objects.create_log(error_description=f'scraping error',error_message=str(e))
            err.send_report_error()
            return None
        curr.index_price = price
        curr.save()
    else:
        identifier = curr.index_ticker.replace('.','^')
        if currency == 'USD':
            identifier = '^GSPC'
        symbol = {
    		"symbols" : identifier
    	}
    	# print(symbol)
        req = requests.get(url, headers=header, params=symbol)
        if req.status_code == 200:
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