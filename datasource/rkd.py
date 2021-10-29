from datetime import datetime
import requests
import json
import math
import pandas as pd
from typing import List,Optional,Union
from core.services.models import ThirdpartyCredentials,ErrorLog
from core.universe.models import ExchangeMarket,Universe
from core.djangomodule.calendar import TradingHours
import sys
import websocket
import aiohttp
import asyncio
import socket
import time
from config.celery import app
import numpy as np
from firebase_admin import firestore
import multiprocessing as mp
import gc
from core.djangomodule.general import logging,jsonprint
from django.conf import settings


db = firestore.client()



def bulk_update_rtdb(data):
    batch = db.batch()
    for ticker,val in data.items():
        if not ticker == 'price':
            ref = db.collection(settings.FIREBASE_COLLECTION['universe']).document(ticker)
            batch.set(ref,val,merge=True)
    try:
        batch.commit()
    except Exception as e:
        err = str(e)
        logging.error(err)

        return f" error : \n {err}"
    logging.info("price updated")
    del ref
    gc.collect()
    return 'ticker bulk updated'


class Rkd:
    token = None

    def __init__(self, *args, **kwargs):
        self.credentials = ThirdpartyCredentials.objects.get(services="RKD")
        self.headers = {"content-type": "application/json;charset=utf-8"}
        self.validate_token()

    async def snapshot_gather_request(self, url, payload, headers):
        async with aiohttp.ClientSession(headers=headers) as session:

            responses = []
            for data in payload:
                responses.append(asyncio.ensure_future(
                    self.async_send_request_snapsot(session, url, data)))

            original_responses = await asyncio.gather(*responses)
            return original_responses

    async def async_send_request_snapsot(self, session, url, payload):
        async with session.post(url, data=json.dumps(payload)) as resp:
            response = await resp.json()
            status = resp.status

            if status == 200:
                return response
            else:
                response = {
                    "ticker": payload["GetRatiosReports_Request_1"]["companyId"],
                    "Error": response,
                    "AREVPS": None,
                    "MKTCAP": None,
                    "PEEXCLXOR": None,
                    "ProjPE": None,
                    "APRICE2BK": None,
                    "AEBITD": None,
                    "NHIG": None,
                    "NLOW": None,
                    "ACFSHR": None,
                }
                return response
    
    
    
    def validate_token(self):
        if self.is_valid_token:
            logging.info("valid token")
            logging.info("using existing token")
            self.token = self.credentials.token
        else:
            self.token = self.get_token()

    def send_request(self, url, payload, headers):
        result = None

        try:
            result = requests.post(
                url, data=json.dumps(payload), headers=headers)
            if result.status_code != 200:

                logging.warning("Request fail")
                logging.warning(f"request url :  {url}")
                logging.warning(f"request header :  {headers}")
                logging.warning(f"response status {result.status_code}")
                if result.status_code == 500:  # if username or password or appid is wrong
                    resp = result.json()
                    logging.warning("Error: %s" % (json.dumps(result.json(),indent=2)))
                    err = json.dumps(result.json(),indent=2)
                    # report =ErrorLog.objects.create(error_description=resp['Fault']['Reason']['Text']['Value'],error_traceback='err',
                    # error_message='Token Invalid',
                    # error_function='RKD DATA')
                    # report.send_report_error()
                    return None
        except requests.exceptions.RequestException as e:
            logging.warning("error : {str(e)}")
            error_log =ErrorLog.objects.create_log(error_description="TRKD REQUEST ERROR",error_message=str(e))
            error_log.send_report()
            raise Exception("request error")
        return result.json()

    def get_token(self):
        authenMsg = {
            "CreateServiceToken_Request_1": {
                "ApplicationID": self.credentials.extra_data["key"],
                "Username": self.credentials.username,
                "Password": self.credentials.password
            }
        }
        authenURL = f"{self.credentials.base_url}TokenManagement/TokenManagement.svc/REST/Anonymous/TokenManagement_1/CreateServiceToken_1"
        logging.info("logged you in")
        logging.info("requesting new token")
        response = self.send_request(authenURL, authenMsg, self.headers)
        if response:
            self.credentials.token = response["CreateServiceToken_Response_1"]["Token"]
            self.credentials.save()
            logging.info("new token saved")
            return self.credentials.token

    @property
    def is_valid_token(self):

        payload = {
            "ValidateToken_Request_1": {
                "ApplicationID": self.credentials.extra_data["key"],
                "Token": self.credentials.token
            }
        }
        headers = self.auth_headers()
        validate_url = f"{self.credentials.base_url}TokenManagement/TokenManagement.svc/REST/TokenManagement_1/ValidateToken_1"
        retry = 0
        while True:
            response = self.send_request(validate_url, payload, headers)
            if response:
                break
            else:
                retry += 1
                self.get_token()
                payload["ValidateToken_Request_1"]["Token"] = self.credentials.token
                headers["X-Trkd-Auth-Token"] = self.credentials.token
                response = self.send_request(validate_url, payload, headers)
            if retry >= 3:
                raise ConnectionError(f"failed after, retry {retry} times")

        return response["ValidateToken_Response_1"]["Valid"]

    def auth_headers(self):
        headers = self.headers
        headers["X-Trkd-Auth-ApplicationID"] = self.credentials.extra_data["key"]
        headers["X-Trkd-Auth-Token"] = self.credentials.token
        return headers

    def parse_response(self, response):
        if response:
            json_data = response.get("RetrieveItem_Response_3",{}).get("ItemResponse",[])
            if json_data:
                data = json_data[0].get("Item",None)
                if not data:
                    logging.error(response)
                    raise Exception(response)
                formated_json_data = []
                for index, item in enumerate(data):
                    ticker = item["RequestKey"]["Name"]
                    formated_json_data.append({"ticker": ticker})
                    if item["Status"]["StatusMsg"] == "OK":
                        for f in item["Fields"]["F"]:
                            field = f["n"]
                            val = f["Value"]
                            formated_json_data[index].update({field: val})
                    else:
                        logging.warning(f"error status message {item['Status']['StatusMsg']} for {ticker}, there is no response data")
            return formated_json_data
        logging.error(response)
        raise Exception(response)


class RkdData(Rkd):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def retrive_template(self, ticker, scope="List", fields=""):

        if scope == "List":
            if not fields or fields == "":
                raise ValueError(
                    "fields keyword argument must set if scope is list")
            field = (",").join(fields)
            fields = field.replace(",", ":")
        payload = {
            "RetrieveItem_Request_3": {
                "ItemRequest": [
                    {
                        "Fields": fields,
                        "RequestKey": [

                        ],
                        "Scope": scope
                    }
                ],
                "TrimResponse": True,
                "IncludeChildItemQoS": False
            }
        }
        if isinstance(ticker, str):
            payload["RetrieveItem_Request_3"]["ItemRequest"][0]["RequestKey"].append(
                {
                    "Name": ticker,
                    "NameType": "RIC"
                })

        elif isinstance(ticker, list):
            for tic in ticker:
                payload["RetrieveItem_Request_3"]["ItemRequest"][0]["RequestKey"].append(
                    {
                        "Name": tic,
                        "NameType": "RIC"
                    }
                )

        return payload

    def get_snapshot(self, ticker, save=False, df=False):
        snapshot_url = f"{self.credentials.base_url}Fundamentals/Fundamentals.svc/REST/Fundamentals_1/GetRatiosReports_1"
        list_formated_json = []
        if isinstance(ticker, list):
            list_payload = []
            for tic in ticker:
                payload = {
                    "GetRatiosReports_Request_1": {
                        "companyId": tic,
                        "companyIdType": "RIC"
                    }
                }
                list_payload.append(payload)
            responses = asyncio.run(self.snapshot_gather_request(
                snapshot_url, list_payload, self.auth_headers()))
            
            for response in responses:
                if not "Error" in response:
                    base_response = response["GetRatiosReports_Response_1"]["FundamentalReports"]["ReportRatios"]

                    formated_json = {}
                    formated_json["ticker"] = base_response["Issues"]["Issue"][0]["IssueID"][2]["Value"]
                    fields = ["AREVPS",
                              "MKTCAP",
                              "PEEXCLXOR",
                              "ProjPE",
                              "APRICE2BK",
                              "AEBITD",
                              "NHIG",
                              "NLOW",
                              "ACFSHR"]
                    for group_item in base_response["Ratios"]["Group"]:
                        for item in group_item["Ratio"]:
                            if item["FieldName"] in fields:
                                formated_json[item["FieldName"]
                                              ] = item["Value"]
                    for group_item in base_response["ForecastData"]["Ratio"]:
                        if group_item["FieldName"] in fields:
                            formated_json[group_item["FieldName"]
                                          ] = group_item["Value"][0]["Value"]
                    list_formated_json.append(formated_json)
                elif "Error" in response:
                    list_formated_json.append(response)
                else:
                    print(response)
        else:
            payload = {
                "GetRatiosReports_Request_1": {
                    "companyId": ticker,
                    "companyIdType": "RIC"
                }
            }
            response = self.send_request(
                snapshot_url, payload, self.auth_headers())
            base_response = response["GetRatiosReports_Response_1"]["FundamentalReports"]["ReportRatios"]

            formated_json = {}
            formated_json["ticker"] = base_response["Issues"]["Issue"][0]["IssueID"][2]["Value"]
            fields = ["AREVPS",
                      "MKTCAP",
                      "PEEXCLXOR",
                      "ProjPE",
                      "APRICE2BK",
                      "AEBITD",
                      "NHIG",
                      "NLOW",
                      "ACFSHR"]
            for group_item in base_response["Ratios"]["Group"]:
                for item in group_item["Ratio"]:
                    if item["FieldName"] in fields:
                        formated_json[item["FieldName"]] = item["Value"]
            for group_item in base_response["ForecastData"]["Ratio"]:
                if group_item["FieldName"] in fields:
                    formated_json[group_item["FieldName"]
                                  ] = group_item["Value"][0]["Value"]
            list_formated_json.append(formated_json)
        df_data = pd.DataFrame(list_formated_json).rename(columns={
            "AREVPS": "revenue_per_share",
            "MKTCAP": "market_cap",
            "PEEXCLXOR": "pe_ratio",
            "ProjPE": "pe_forecast",
            "APRICE2BK": "pb",
            "AEBITD": "ebitda",
            "NHIG": "wk52_high",
            "NLOW": "wk52_low",
            "ACFSHR": "free_cash_flow",
        })
        if save:
            self.save("universe", "Universe", df_data.to_dict("records"))
        if df:
            return df_data
        return formated_json
    
    def get_data_from_rkd(self, identifier, field):
        quote_url = f"{self.credentials.base_url}Quotes/Quotes.svc/REST/Quotes_1/RetrieveItem_3"
        split = round(len(identifier) / min(50, len(identifier)))
        collected_data =[]
        splitting_df = np.array_split(identifier, max(split, 1))
        for universe in splitting_df:
            tick = universe.tolist()
            payload = self.retrive_template(tick, fields=field)
            response = self.send_request(quote_url, payload, self.auth_headers())
            formated_json_data = self.parse_response(response)
            result = pd.DataFrame(formated_json_data)
            collected_data.append(result)
        collected_data = pd.concat(collected_data)
        return result

    def get_index_price(self,currency:str):
        from django.apps import apps
        Model = apps.get_model("universe", "Currency")
        currency = Model.objects.get(currency_code=currency)
        quote_url = f"{self.credentials.base_url}Quotes/Quotes.svc/REST/Quotes_1/RetrieveItem_3"
        payload = self.retrive_template(currency.index_ticker, fields=["CF_CLOSE"])
        response = self.send_request(quote_url, payload, self.auth_headers())

        formated_json_data = self.parse_response(response)
        df_data = pd.DataFrame(formated_json_data).rename(columns={
            "CF_CLOSE": "index_price",
        })
        price = formated_json_data[0].get("CF_CLOSE", currency.index_price)
        currency.index_price = price
        currency.save()

    def response_to_df(self,response:dict) -> pd.DataFrame:
        formated_json_data = self.parse_response(response)
        # jsonprint(formated_json_data)
        float_fields = [
            'CF_ASK',
            'CF_CLOSE',
            'CF_BID',
            'CF_HIGH',
            'CF_LOW',
            'PCTCHNG',
            'CF_VOLUME',
            'CF_LAST',
            'CF_NETCHNG',
            'CF_OPEN',
            'YIELD'
            ]
        parser_data ={
                "CF_ASK": "intraday_ask",
                "CF_OPEN": "open",
                "CF_CLOSE": "close",
                "CF_BID": "intraday_bid",
                "CF_HIGH": "high", 
                "CF_LOW": "low",
                "PCTCHNG": "latest_price_change",
                "TRADE_DATE": "last_date",
                "CF_VOLUME": "volume",
                "CF_LAST": "latest_price",
                "CF_NETCHNG": "latest_net_change",
                "YIELD": "dividen_yield",
                }
        float_data = {
                "intraday_ask":"float",
                "close":"float",
                "open":"float",
                "intraday_bid":"float",
                "high":"float",
                "low":"float",
                "latest_price_change":"float",
                "volume":"float",
                "latest_price":"float",
                "latest_net_change":"float",
                "dividen_yield":"float",
            }
        
        for parsed_data in formated_json_data:
            for field in float_fields:
                values=parsed_data.get(field,0)
                if field == "CF_LAST" and float(values) == 0:
                    values=parsed_data.get("CF_CLOSE",0)
                if field == "CF_OPEN" and float(values) == 0:
                    values=parsed_data.get("CF_LAST",0)
                if float(values) != 0:
                    parsed_data[field]=values
        df_data = pd.DataFrame(formated_json_data)
        df_data = df_data.rename(columns={k: v for k, v in parser_data.items() if k  in df_data})
        # print(df_data)
        df_data["last_date"] = str(datetime.now().date())
        df_data["intraday_time"] = str(datetime.now())
        df_data =df_data.astype(
            {k: v for k, v in float_data.items() if k  in df_data}
        )
        return df_data

    async def quote_gather_request(self, url:str, payload:list, headers:dict)-> List[pd.DataFrame]:
        async with aiohttp.ClientSession(headers=headers) as session:

            responses = []
            for data in payload:
                responses.append(asyncio.ensure_future(
                    self.async_send_request_quote(session, url, data)))

            original_responses = await asyncio.gather(*responses)
            return original_responses

    async def async_send_request_quote(self, session, url, payload) -> pd.DataFrame:
        async with session.post(url, data=json.dumps(payload)) as resp:
            response = await resp.json()
            status = resp.status

            if status == 200:
                return self.response_to_df(response)
            else:
                raise Exception(response)

    def bulk_get_quote(self, ticker:list, df=False, save=False,**options)->Optional[Union[pd.DataFrame, dict]] :
        quote_url = f'{self.credentials.base_url}Quotes/Quotes.svc/REST/Quotes_1/RetrieveItem_3'
        split = len(ticker)/50
        if split < 2:
            split = math.ceil(split)
        splitting_df = np.array_split(ticker, split)
        bulk_payload=[]
        for universe in splitting_df:
            ticker = universe.tolist()
            payload = self.retrive_template(ticker, fields=[
                                            "CF_ASK","CF_OPEN", "CF_CLOSE", "CF_BID", "PCTCHNG", "CF_HIGH", "CF_LOW", "CF_LAST", 
                                            "CF_VOLUME", "TRADE_DATE","CF_NETCHNG","YIELD"])
            bulk_payload.append(payload)
        
        self.validate_token()
        
        response:List[pd.DataFrame] = asyncio.run(self.quote_gather_request(quote_url,bulk_payload,self.auth_headers()))
        data : pd.DataFrame = pd.concat(response,ignore_index=True)
        if df:
            return data
        return data.to_dict("records")




    def get_quote(self, ticker, df=False, save=False,**options):
        import math
        if isinstance(ticker,str):
            ticker = [ticker]
        quote_url = f'{self.credentials.base_url}Quotes/Quotes.svc/REST/Quotes_1/RetrieveItem_3'
        split = len(ticker)/50
        collected_data =[]
        if split < 2:
            split = math.ceil(split)
        splitting_df = np.array_split(ticker, split)
        for universe in splitting_df:
            ticker = universe.tolist()
            print(len(ticker))
            payload = self.retrive_template(ticker, fields=[
                                            "CF_ASK","CF_OPEN", "CF_CLOSE", "CF_BID", "PCTCHNG", "CF_HIGH", "CF_LOW", "CF_LAST", 
                                            "CF_VOLUME", "TRADE_DATE","CF_NETCHNG","YIELD"])
            response = self.send_request(quote_url, payload, self.auth_headers())

            formated_json_data = self.parse_response(response)
            df_data = pd.DataFrame(formated_json_data).rename(columns={
                "CF_ASK": "intraday_ask",
                "CF_CLOSE": "close",
                "CF_OPEN": "open",
                "CF_BID": "intraday_bid",
                "CF_HIGH": "high", 
                "CF_LOW": "low",
                "PCTCHNG": "latest_price_change",
                "TRADE_DATE": "last_date",
                "CF_VOLUME": "volume",
                "CF_LAST": "latest_price",
                "CF_NETCHNG": "latest_net_change",
                "YIELD":"dividen_yield"
            })
            df_data["last_date"] = str(datetime.now().date())
            df_data["intraday_date"] = str(datetime.now().date())
            df_data["intraday_time"] = str(datetime.now())
            collected_data.append(df_data)
        collected_data = pd.concat(collected_data,ignore_index=True)
        if save:
            print("saving....")
            self.save("master", "LatestPrice", collected_data.to_dict("records"))
            if "detail" in options:
                types = options["detail"].split("-")[0]
                collected_data["types"] = types
                collected_data["hedge_uid"]=collected_data["ticker"].astype(str)+options["detail"]
                collected_data["hedge_uid"]=collected_data["hedge_uid"].str.replace("-", "", regex=True).str.replace(".", "", regex=True).str.strip()
                self.save("master","HedgeLatestPriceHistory",collected_data.to_dict("records"))
            print("saving done")

        if df:
            # rename column match in table
            return collected_data
        return collected_data.to_dict("records")

    def get_rkd_data(self,ticker,save=False):
        """getting all data from RKD and save,function has no return"""

        self.get_snapshot(ticker,save=save)
        self.get_quote(ticker,save=save)


    @app.task(bind=True,ignore_result=True)
    def save(self, app, model, data):
        from django.apps import apps
        Model = apps.get_model(app, model)
        pk = Model._meta.pk.name
        if isinstance(data, list):
            if isinstance(data[0], dict):
                pass
            else:
                raise Exception("data should be dataframe or list of dict")
        elif isinstance(data, pd.DataFrame):
            data = data.to_dict("records")
        else:
            raise Exception("data should be dataframe or dict")
        key_set = [key for key in data[0].keys()]
        list_obj = []
        create =False
        for item in data:
            if pk in key_set:
                key_set.remove(pk)
            try:
                key = {pk: item[pk]}
                obj = Model.objects.get(**key)
            except Model.DoesNotExist:
                print(f"models {item[pk]} does not exist")
                if app != "LatestPrice":
                    create=True
                    
            except KeyError:
                raise Exception("no primary key in dict")
            if not create:
                for attr, val in item.items():
                    if hasattr(obj,attr):
                        field = obj._meta.get_field(attr)
                        if field.one_to_many or field.many_to_many or field.many_to_one or field.one_to_one:
                            attr = f"{attr}_id"
                        setattr(obj, attr, val)
            else:
                attribs_modifier = {}
                for attr, val in item.items():
                    if hasattr(Model,attr):
                        field = Model._meta.get_field(attr)
                        if field.one_to_many or field.many_to_many or field.many_to_one or field.one_to_one:
                            attribs_modifier[f"{attr}_id"] = val
                        else:
                            attribs_modifier[attr]=val
                    
                    obj =Model(**attribs_modifier)
            list_obj.append(obj)
        if create and model != "LatestPrice":
            try:
                Model.objects.bulk_create(list_obj,ignore_conflicts=True)
            except Exception:
                pass
        elif not create and model == "LatestPrice":
            for key in key_set:
                if not hasattr(Model,key):
                    key_set.remove(key)
            Model.objects.bulk_update(list_obj, key_set)

class RkdStream(RkdData):
    ID =[]
    chanels = None
    is_thread =False
    exchange_list = []
    exchange = []
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs
        super().__init__(*args, **kwargs)
        self.user = self.credentials.username
        self.app_id = self.credentials.extra_data["key"]
        self.password = self.credentials.password
        self.position = socket.gethostbyname(socket.gethostname())
        self.token = self.credentials.token
        if not args:
            self.ticker_data=[]
        else:
            self.ticker_data = args[0]
        self.ws_address = "wss://streaming.trkd.thomsonreuters.com/WebSocket"

        self.web_socket_app = websocket.WebSocketApp(self.ws_address, header=["User-Agent: Python"],
                                                     on_message=self.on_message,
                                                     on_error=self.on_error,
                                                     on_close=self.on_close,
                                                     on_open = self.on_open,
                                                     subprotocols=["tr_json2"])
        if not self.ticker_data:
            exchange = self.get_list_exchange()
            self.ticker_data = [data['ticker'] for data in Universe.objects.filter(mic__in=exchange, is_active=True).values('ticker')]
        # self.layer = get_channel_layer()
    
    @classmethod
    def trkd_stream_initiate(cls,ticker):
        return cls(ticker)
    
    def thread_stream_quote(self):
        from django import db
        db.connections.close_all()
        threads = mp.Process(target=self.stream_quote)
        threads.name = self.chanels
        self.is_thread =True
        return threads

    def stream_quote(self):
        # FOR NOW ONLY HKD
        # TODO: Need to enhance this
        if not settings.RUN_LOCAL:
            while True:
                open_market=(ExchangeMarket.objects.filter(currency_code__in=["HKD","USD"],
                group="Core",is_open=True).values_list("currency_code",flat=True))
                # usd_exchange,hkd_exchange =ExchangeMarket.objects.filter(mic='XNAS'),ExchangeMarket.objects.get(mic='XHKG')
                if open_market:
                    self.ticker_data =list(Universe.objects.filter(currency_code__in=open_market, 
                    is_active=True).exclude(Error__contains='{').values_list('ticker',flat=True))
                    logging.info('stream price')
                    data =self.bulk_get_quote(self.ticker_data,df=True)
                    split_df = np.array_split(data,math.ceil(len(data)/400))
                    for data_split in split_df:
                        df = data_split.copy()
                        data_split['price'] = df.drop(columns=['ticker']).to_dict("records")
                        del df
                        data_split = data_split[['ticker','price']]
                        data_split = data_split.set_index('ticker')
                        records = data_split.to_dict("index")
                        # logging.info(records)
                        
                        bulk_update_rtdb(records)
                        del records
                        del data_split
                    del data
                    del split_df
                    gc.collect()
                else:
                    break
                time.sleep(15)
        if self.is_thread:
                sys.exit()
        


    
    def thread_stream(self):
        from django import db
        db.connections.close_all()
        threads = mp.Process(target=self.stream)
        threads.name = self.chanels
        self.is_thread =True
        return threads

    def stream(self):
        # print("Connecting to WebSocket " + self.ws_address + " ...")
        try:
            self.web_socket_app.run_forever()
        except KeyboardInterrupt as e:
            print(f"==========={e}=============")
            print("interupted")
            self.web_socket_app.close()
            if self.is_thread:
                sys.exit()
        except Exception as e:
            print(f"==========={e}=============")
            self.web_socket_app.close()
            
            if self.is_thread:
                sys.exit()
    
    
    def get_list_exchange(self):
        if not self.exchange_list:
            self.exchange_list = [exc['mic'] for exc in ExchangeMarket.objects.filter(currency_code__in=['HKD','USD'],is_open=True).values('mic')]
            self.exchange =self.exchange_list
        return self.exchange_list


    def answer_ping(self, ws, *args, **options):
        ping_req = {"Type": "Pong"}
        current_live_exchange = len(self.exchange_list)
        for exc in self.exchange:
            market = TradingHours(mic=exc)
            if market.is_open:
                if not exc in self.exchange_list:
                    self.exchange_list.append(exc)
            else:
                if exc in self.exchange_list:
                    self.exchange_list.remove(exc)
        
        
        
        
        if self.exchange_list:
            ws.send(json.dumps(ping_req))
            print("SENT:")
            print(json.dumps(ping_req, sort_keys=True,
                            indent=2, separators=(",", ":")))
            if current_live_exchange != len(self.exchange_list):
                self.unsubscibe_event(ws)
                self.ticker_data = [data['ticker'] for data in Universe.objects.filter(mic__in=self.exchange_list, is_active=True).values('ticker')]
                self.send_market_price_request(ws)
        else:
            self.on_close(ws)

    def write_on_s3(self, message, *args, **options):
        try:
            ticker = message["Key"]["Name"]
            type_name = message["Type"]
            act = message["UpdateType"]
            note = json.dumps(message)
            s3 = boto3.client("s3", aws_access_key_id="AKIA2XEOTUNGWEQ43TB6",
                              aws_secret_access_key="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", region_name="ap-east-1")
            # epoch = str(int(datetime.now().timestamp()))
            # s3_file = "test_dir/"+str(int(epoch)) + ".txt"
            s3_file = "test_dir/"+type_name+"-"+ticker+"-"+act+".txt"
            upload = s3.put_object(
                Body=note, Bucket="droid-v2-logs", Key=s3_file)
        except Exception as e:
            print(f"Error ===== {e}")

    def process_message(self, ws, message_json, *args, **options):
        """ Parse at high level and output JSON of message """
        message_type = message_json["Type"]
        # print(f"Message json ====== {message_json}")

        """ check for login response """
        if message_type == "Refresh":
            if "Domain" in message_json:
                message_domain = message_json["Domain"]
                if message_domain == "Login":
                    self.process_login_response(ws, message_json)
                    return
            else:
                # self.write_on_s3(message_json)
                pass
        elif message_type == "Ping":
            self.answer_ping(ws)
            """CREATING MARKET CHECK"""

        elif message_type == "Update":
            if message_json['UpdateType'] == 'Quote':
                # self.rkd.save('master', 'LatestPrice', data)
                print(
                    f"====== Quote - {message_json['Key']['Name']} ======")
                self.beautify_print(message_json, **options)
            elif message_json['UpdateType'] == 'Trade':
                print(
                    f"====== Trade - {message_json['Key']['Name']} ======")
                self.beautify_print(message_json, **options)
            elif message_json['UpdateType'] == 'Unspecified':
                print(
                    f"====== Unspecified - {message_json['Key']['Name']} ======")
                self.beautify_print(message_json, **options)
            else:
                None
            # write_on_s3(message_json)

        """ record ID """
        if "ID" in message_json:
            if message_json["ID"] not in self.ID:
                self.ID.append(message_json["ID"])
        # web_socket_app.close()

    def process_login_response(self, ws, message_json, *args, **options):
        print("Logged in!")
        
        self.send_market_price_request(ws)

    def beautify_print(self, message, *args, **options):
        change = {
            "CF_ASK": "intraday_ask",
            "CF_CLOSE": "close",
            "CF_BID": "intraday_bid",
            "CF_HIGH": "high", 
            "CF_LOW": "low",
            "PCTCHNG": "latest_price_change",
            "TRADE_DATE": "last_date",
            "CF_VOLUME": "volume",
            "CF_LAST": "latest_price",
            "CF_NETCHNG": "latest_net_change"
        }
        if "PCTCHNG" in message["Fields"]:
            message["Fields"]["ticker"] = message["Key"]["Name"]
            data = [message["Fields"]]
            df = pd.DataFrame(data).rename(columns=change)
            # ticker = df.loc[df["ticker"] == message["Fields"]["ticker"]]
            print(df)
            self.update_rtdb.apply_async(args=(df.to_dict("records"),),queue="broadcaster")

            del df
            # del ticker
            gc.collect()

    def send_market_price_request(self, ws, *args, **options):
        """ Create and send simple Market Price request """

        mp_req_json = {
            "ID": int(time.time()),
            "Key": {
                "Name": self.ticker_data
            },
            "View": [
                "PCTCHNG",
                "CF_CLOSE",
                "CF_ASK",
                "CF_BID",
                "CF_HIGH",
                "CF_LOW",
                "CF_LAST",
                "CF_VOLUME",
                "CF_NETCHNG",
                "TRADE_DATE"
            ]
        }

        ws.send(json.dumps(mp_req_json))
        logging.info("SENT:")
        # logging.info(json.dumps(mp_req_json, sort_keys=True,
        #                  indent=2, separators=(",", ":")))

    def send_login_request(self, ws, *args, **options):
        """ Generate a login request from command line data (or defaults) and send """
        login_json = {"ID": 1, "Domain": "Login",
                      "Key": {"NameType": "AuthnToken", "Name": "",
                              "Elements": {"AuthenticationToken": "", "ApplicationId": "", "Position": ""}
                              }
                      }
        login_json["Key"]["Name"] = self.user
        login_json["Key"]["Elements"]["AuthenticationToken"] = self.token
        login_json["Key"]["Elements"]["ApplicationId"] = self.app_id
        login_json["Key"]["Elements"]["Position"] = self.position
        ws.send(json.dumps(login_json))
        logging.info("SENT LOGIN REQUEST:")
        # print(json.dumps(login_json, sort_keys=True,
        #                  indent=2, separators=(",", ":")))

    def on_message(self, ws, message, *args, **options):
        """ Called when message received, parse message into JSON for processing """
        # print("")
        # print("======== RECEIVED: ========")
        # print(f"ws == {ws}, msg == {message}, args == {args}, option == {options}")
        message_json = json.loads(message)
        # print(json.dumps(message_json, sort_keys=True, indent=2, separators=(",", ":")))
        
        for singleMsg in message_json:
            self.process_message(ws, singleMsg)

    def on_error(self, ws, error, *args, **options):
        """ Called when websocket error has occurred """
        logging.error(error)
        ws.close()
        if self.is_thread:
            sys.exit()

    def on_close(self, ws, *args, **options):
        # print(super(on_close, self))
        """ Called when websocket is closed """
        logging.info("WebSocket Closed")
        ws.close()
        if self.is_thread:
            sys.exit()

    def on_open(self, ws, *args, **options):
        """ Called when handshake is complete and websocket is open, send login """
        logging.info("WebSocket open!")
        
        self.send_login_request(ws)
    
    def unsubscibe_event(self,ws):
        payload={
                "Type": "Close",
                "ID": self.ID
                    }
        logging.info(payload)
        ws.send(json.dumps(payload))

    @app.task(bind=True,ignore_result=True)
    def update_rtdb(self,data):
        data = data[0]
        ticker = data.pop("ticker")
        logging.info(ticker)
        ref = db.collection(settings.FIREBASE_COLLECTION['universe']).document(ticker)
        try:
            ref.set({"price":data},merge=True)
        except Exception as e:
            err = str(e)
            logging.error(err)
            return f"{ticker} error : \n {err}"
        del ref
        gc.collect()
        return ticker
    
    @app.task()
    def bulk_update_rtdb(data):
        batch = db.batch()
        logging.info(data)
        for ticker,val in data.items():
            if not ticker == 'price':
                ref = db.collection(settings.FIREBASE_COLLECTION['universe']).document(ticker)
                batch.set(ref,val,merge=True)
        try:
            batch.commit()
        except Exception as e:
            err = str(e)
            logging.error(err)
            return f" error : \n {err}"
        logging.info("price updated")
        del ref
        gc.collect()
        return 'ticker bulk updated'