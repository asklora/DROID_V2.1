from django.core.management.base import BaseCommand, CommandError
from core.services.upload_to_s3 import s3_uploader
from bs4 import BeautifulSoup as soup
from core.universe.models import ExchangeMarket, CurrencyCalendars
import cfscrape
from datetime import datetime
class Command(BaseCommand):

    def add_argument(self, parser):
        parser.add_argument("-mic_id", "--mic_id", type=str, help="mic_id",)

    def handle(self, *args, **options):
        # print(f"===={args}==={options}")
        # if options['mic_id']:
        #     market = ExchangeMarket.objects.filter(mic=options['mic_id'])
        #     print(market)
        # else:
        market = ExchangeMarket.objects.all()
        json_data = {}
        for data_market in market:
            if data_market.currency_code == None:
                # finding = False
                # print("SKIP")
                pass
            else:
                # finding=True
                # print(f"===================={data_market.currency_code}==================")
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
                elif data_market.mic == 'MUNA':
                    exchange = 'XMUN'
                elif data_market.mic == 'STUA':
                    exchange = 'XSTU'
                elif data_market.mic == 'XBRV':
                    exchange = 'BRVM'
                elif data_market.mic == 'XBAB':
                    exchange = 'BDE-BARBADOS'
                elif data_market.mic == 'XBUL':
                    exchange = 'BSE-BULGARIA'
                elif data_market.mic == 'XBOT':
                    exchange = 'BSE-BOTSWANA'
                elif data_market.mic == 'TGAT':
                    exchange = 'XETR'
                elif data_market.mic == 'XTAL':
                    exchange = 'OMXT-TALLIN'
                elif data_market.mic == 'XCAI':
                    exchange = 'EGX'
                elif data_market.mic == 'XLME':
                    exchange = 'LOTC'
                elif data_market.mic == 'ASEX':
                    exchange = 'ASE-ATHENS'
                elif data_market.mic == 'XBUD':
                    exchange = 'BSE-BUDAPEST'
                elif data_market.mic == 'XBOM':
                    exchange = 'BSE-BOMBAY'
                elif data_market.mic == 'XNSE':
                    exchange = 'BSE-INDIA'
                elif data_market.mic == 'XICE':
                    exchange = 'XICE'
                elif data_market.mic == 'XAMM':
                    exchange = 'ASE-AMMAN'
                elif data_market.mic == 'XNGO':
                    exchange = 'XNGO'
                elif data_market.mic == 'XNAI':
                    exchange = 'NSE-NAIROBI'
                elif data_market.mic == 'XBEY':
                    exchange = 'BSE-BEIRUT'
                elif data_market.mic == 'XLIT':
                    exchange = 'OMXV-VILNIUS'
                elif data_market.mic == 'XRIS':
                    exchange = 'OMXR-RIGA'
                elif data_market.mic == 'XNSA':
                    exchange = 'NSE-NIGERIA'
                elif data_market.mic == 'NOTC':
                    exchange = 'OSE'
                elif data_market.mic == 'XPTY':
                    exchange = 'BVPA'
                elif data_market.mic == 'XSAF':
                    exchange = 'JSE'
                elif data_market.mic == 'XTRN':
                    print("data notfound for trinidad and tobago republic. ")
                    pass
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

                # print(data_market.mic, " - " ,exchange_symbol)
                print(f"=================={data_market.mic}=====================")
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
                        if "†" in dates:
                            dates = dates.replace("†","")
                        if "(past)" in dates:
                            dates = dates.replace("(past)","")
                        to_time = datetime.strptime(dates.strip(), "%A, %B %d, %Y")
                        to_str = to_time.strftime("%Y-%m-%d")
                        holiday = {
                            "holiday":excuse,
                            "date":to_str
                        }
                        json_data[data_market.mic].append(holiday)
                        print(f'{data_market.mic} -- {to_str} -- {excuse}')
                        holiday=CurrencyCalendars(
                            uid=to_str.replace("-","")+data_market.mic,
                            currency_code=data_market.currency_code,
                            non_working_day=to_time,
                            description=excuse
                            )
                        holiday.save()
                        # print("============================================")
                except Exception as e:
                    print(f'{data_market.mic} -- {e}')
                    error = {"holiday":None, 'date':None, 'error':str(e)}
                    json_data[data_market.mic].append(error)

                # print(exchange)
        # print(json_data)
        ####################################################
