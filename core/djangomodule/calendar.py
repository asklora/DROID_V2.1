import requests
import json
import ast
from datetime import datetime, timedelta
import pytz
from core.universe.models import ExchangeMarket
import pandas as pd




class TradingHours:
    next_bell = None
    token = "1M1a35Qhk8gUbCsOSl6XRY2z3Qjj0of7y5ZEfE5MasUYm5b9YsoooA7RSxW7"
    market_timezone = None

    def __init__(self, fins=None,mic=None):
        if mic:
            if isinstance(mic,str):
                exchange = ExchangeMarket.objects.get(mic=mic)
                self.fin_id =exchange.fin_id
                self.get_market_timezone()
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
    
    
    
    
    def get_market_timezone(self):
        url =f'https://api.tradinghours.com/v3/markets/details?fin_id={self.fin_id}&token={self.token}'
        req = requests.get(url)
        res = req.json()
        if req.status_code == 200:
            self.market_timezone=res['data'][0]['timezone']
    
    def timezone_to_utc(self,dt,timezone):
        time_zone = pytz.timezone(timezone)
        date_time = time_zone.localize(dt)
        return date_time.astimezone(pytz.utc)
    
    
    def get_index_symbol(self):
        url = "https://api.tradinghours.com/v3/markets?group=core&token="+self.token
        req = requests.get(url)
        res = req.json()
        return res

    def get_weekly_holiday(self):
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
                try:
                    len_date = len(resp['data'][self.fin_id]['next_bell']) - 6
                    local_time = pd.to_datetime(resp['data'][self.fin_id]['next_bell'][:len_date])
                    print(local_time,self.market_timezone)
                    self.next_bell =self.timezone_to_utc(local_time,self.market_timezone)
                    print(self.next_bell)
                except Exception:
                    pass
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
