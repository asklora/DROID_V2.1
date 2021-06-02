from django.core.management.base import BaseCommand, CommandError
from core.services.upload_to_s3 import s3_uploader
from bs4 import BeautifulSoup as soup
from core.universe.models import ExchangeMarket
import cfscrape

class Command(BaseCommand):

    def handle(self, *args, **options):
        # market = TradingHours(mic='XNAS')
        # print(market.is_open)
        # mics = [
        #     'XEUR',
        #     'XMAD',
        #     'XPAR',
        #     'XTAI',
        #     'XSHE',
        #     'XNAS',
        #     'XHEL',
        #     'XLIS',
        #     'XETA',
        #     'XBRU',
        #     'XAMS',
        #     'XKRX',
        #     'XMIL',
        #     'FRAA',
        #     'XHKG',
        #     'XMSM',
        #     'XSHG'
        # ]
        # holiday = avail_holiday()
        # for mic in mics:
        #     print(f"===================={mic}===========================")
        #     try:
        #         market = ExchangeMarket.objects.get(mic=mic)
        #         exc = market.fin_id
        #         dot = exc.find(".")
        #         exchange_symbol = exc[dot:].replace(".","")
        #         # try:
        #         print(exchange_symbol)
        #         if len(holiday[exchange_symbol]) == 1:
        #             print(holiday[exchange_symbol])
        #     except ExchangeMarket.DoesNotExist:
        #         print(f"================== Does Not Exist ========================")

        ####################################################
        market = ExchangeMarket.objects.all()
        json_data = {}
        for data_market in market:
            ###################### Special MIC ##########################
            if data_market.mic == 'XMSM':
                pass
            elif data_market.mic == 'XPAR':
                exchange = 'EURONEXT-PARIS'
            elif data_market.mic == 'XHEL':
                exchange = 'OMXH-HELSINKI'
            elif data_market.mic == 'XLIS':
                exchange = 'EURONEXT-LISBON'
            elif data_market.mic == 'XBRU':
                exchange = 'EURONEXT-BRUSSELS'
            else:
                exchange = data_market.fin_id

            dot = exchange.find(".")
            if dot != -1:
                exchange_symbol = exchange[dot:].replace(".","")
                if exchange_symbol == 'MIB':
                    exchange_symbol = 'MTA'
                elif exchange_symbol == 'XFRA':
                    exchange_symbol = 'FSX'
            else:
                exchange_symbol = exchange

            print(data_market.mic, " - " ,exchange_symbol)
            json_data[data_market.mic] = []
            url = "https://www.tradinghours.com/markets/"+exchange_symbol.lower()+"/holidays"
            try:
                ########################## request HTML ############################
                scraper = cfscrape.create_scraper()
                scrap = scraper.get(url)
                scrap = scrap.content.decode('utf-8')
                ######################## BS4 ###########################
                page_soup = soup(scrap, "html.parser")
                tb = page_soup.find("div",{"class":"card mb-3"})
                tbody = tb.find("tbody")
                rows = tbody.find_all('tr')
                for row in rows:
                    # print("============================================")
                    excuse = row.find('th').text
                    dates = row.find_all('td')[0].text.replace("\n"," ")
                    dates = dates.replace("†","")
                    holiday = {
                        "holiday":excuse,
                        "date":dates.span()
                    }
                    json_data[data_market.mic].append(holiday)
                    # print("============================================")
            except Exception as e:
                error = {"holiday":None, 'date':None, 'error':str(e)}
                json_data[data_market.mic].append(error)

                # print(exchange)
        print(json_data)
        ####################################################
