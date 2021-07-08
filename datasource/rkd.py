import requests
import json
import pandas as pd
from requests.api import head
from core.services.models import ThirdpartyCredentials
from core.master.models import LatestPrice
import sys
import logging
import websocket
import aiohttp
import asyncio
import socket
import time
from channels.layers import get_channel_layer
from config.celery import app
import numpy as np
from firebase_admin import firestore



logging.basicConfig(format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')
logging.getLogger().setLevel(logging.INFO)

db = firestore.client()
class Rkd:
    token = None

    def __init__(self, *args, **kwargs):
        self.credentials = ThirdpartyCredentials.objects.get(services='RKD')
        self.headers = {'content-type': 'application/json;charset=utf-8'}
        if self.is_valid_token:
            logging.info('valid token')
            logging.info('using existing token')
            self.token = self.credentials.token
        else:
            self.token = self.get_token()

    async def gather_request(self, url, payload, headers):
        async with aiohttp.ClientSession(headers=headers) as session:

            responses = []
            for data in payload:
                responses.append(asyncio.ensure_future(
                    self.async_send_request(session, url, data, headers)))

            original_responses = await asyncio.gather(*responses)
            return original_responses

    async def async_send_request(self, session, url, payload, headers):
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
                    "APEEXCLXOR": None,
                    "ProjPE": None,
                    "APRICE2BK": None,
                    "AEBITD": None,
                    "NHIG": None,
                    "NLOW": None,
                    "ACFSHR": None,
                }
                return response

    def send_request(self, url, payload, headers):
        result = None

        try:
            result = requests.post(
                url, data=json.dumps(payload), headers=headers)
            if result.status_code != 200:

                logging.warning('Request fail')
                logging.warning(f'request url :  {url}')
                logging.warning(f'request header :  {headers}')
                logging.warning(f'request payload :  {payload}')
                logging.warning(f'response status {result.status_code}')
                if result.status_code == 500:  # if username or password or appid is wrong
                    logging.warning('Error: %s' % (result.json()))
                    return None
        except requests.exceptions.RequestException as e:
            logging.warning('error : {str(e)}')
            raise Exception('request error')
        return result.json()

    def get_token(self):
        authenMsg = {
            'CreateServiceToken_Request_1': {
                'ApplicationID': self.credentials.extra_data['key'],
                'Username': self.credentials.username,
                'Password': self.credentials.password
            }
        }
        authenURL = f'{self.credentials.base_url}TokenManagement/TokenManagement.svc/REST/Anonymous/TokenManagement_1/CreateServiceToken_1'
        logging.info('logged you in')
        logging.info('requesting new token')
        response = self.send_request(authenURL, authenMsg, self.headers)
        self.credentials.token = response['CreateServiceToken_Response_1']['Token']
        self.credentials.save()
        logging.info('new token saved')
        return self.credentials.token

    @property
    def is_valid_token(self):

        payload = {
            "ValidateToken_Request_1": {
                "ApplicationID": self.credentials.extra_data['key'],
                "Token": self.credentials.token
            }
        }
        headers = self.auth_headers()
        validate_url = f'{self.credentials.base_url}TokenManagement/TokenManagement.svc/REST/TokenManagement_1/ValidateToken_1'
        retry = 0
        while True:
            response = self.send_request(validate_url, payload, headers)
            if response:
                break
            else:
                retry += 1
                self.get_token()
                payload['ValidateToken_Request_1']['Token'] = self.credentials.token
                headers['X-Trkd-Auth-Token'] = self.credentials.token
                response = self.send_request(validate_url, payload, headers)
            if retry >= 3:
                raise ConnectionError(f'failed after, retry {retry} times')

        return response['ValidateToken_Response_1']['Valid']

    def auth_headers(self):
        headers = self.headers
        headers['X-Trkd-Auth-ApplicationID'] = self.credentials.extra_data['key']
        headers['X-Trkd-Auth-Token'] = self.credentials.token
        return headers

    def parse_response(self, response):
        json_data = response['RetrieveItem_Response_3']['ItemResponse'][0]['Item']
        formated_json_data = []
        for index, item in enumerate(json_data):
            ticker = item['RequestKey']['Name']
            formated_json_data.append({'ticker': ticker})
            for f in item['Fields']['F']:
                field = f['n']
                val = f['Value']
                formated_json_data[index].update({field: val})
        return formated_json_data


class RkdData(Rkd):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def retrive_template(self, ticker, scope="List", fields=''):

        if scope == 'List':
            if not fields or fields == '':
                raise ValueError(
                    'fields keyword argument must set if scope is list')
            field = (',').join(fields)
            fields = field.replace(',', ':')
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
            payload['RetrieveItem_Request_3']['ItemRequest'][0]['RequestKey'].append(
                {
                    "Name": ticker,
                    "NameType": "RIC"
                })

        elif isinstance(ticker, list):
            for tic in ticker:
                payload['RetrieveItem_Request_3']['ItemRequest'][0]['RequestKey'].append(
                    {
                        "Name": tic,
                        "NameType": "RIC"
                    }
                )

        return payload

    def get_snapshot(self, ticker, save=False, df=False):
        snapshot_url = f'{self.credentials.base_url}Fundamentals/Fundamentals.svc/REST/Fundamentals_1/GetRatiosReports_1'
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
            responses = asyncio.run(self.gather_request(
                snapshot_url, list_payload, self.auth_headers()))
            
            for response in responses:
                if not "Error" in response:
                    base_response = response['GetRatiosReports_Response_1']['FundamentalReports']['ReportRatios']

                    formated_json = {}
                    formated_json['ticker'] = base_response['Issues']['Issue'][0]['IssueID'][2]['Value']
                    fields = ['AREVPS',
                              'MKTCAP',
                              'APEEXCLXOR',
                              'ProjPE',
                              'APRICE2BK',
                              'AEBITD',
                              'NHIG',
                              'NLOW',
                              'ACFSHR']
                    for group_item in base_response['Ratios']['Group']:
                        for item in group_item['Ratio']:
                            if item['FieldName'] in fields:
                                formated_json[item['FieldName']
                                              ] = item['Value']
                    for group_item in base_response['ForecastData']['Ratio']:
                        if group_item['FieldName'] in fields:
                            formated_json[group_item['FieldName']
                                          ] = group_item['Value'][0]['Value']
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
            base_response = response['GetRatiosReports_Response_1']['FundamentalReports']['ReportRatios']

            formated_json = {}
            formated_json['ticker'] = base_response['Issues']['Issue'][0]['IssueID'][2]['Value']
            fields = ['AREVPS',
                      'MKTCAP',
                      'APEEXCLXOR',
                      'ProjPE',
                      'APRICE2BK',
                      'AEBITD',
                      'NHIG',
                      'NLOW',
                      'ACFSHR']
            for group_item in base_response['Ratios']['Group']:
                for item in group_item['Ratio']:
                    if item['FieldName'] in fields:
                        formated_json[item['FieldName']] = item['Value']
            for group_item in base_response['ForecastData']['Ratio']:
                if group_item['FieldName'] in fields:
                    formated_json[group_item['FieldName']
                                  ] = group_item['Value'][0]['Value']
            list_formated_json.append(formated_json)
        df_data = pd.DataFrame(list_formated_json).rename(columns={
            'AREVPS': 'revenue_per_share',
            'MKTCAP': 'market_cap',
            'APEEXCLXOR': 'pe_ratio',
            'ProjPE': 'pe_forecast',
            'APRICE2BK': 'pb',
            'AEBITD': 'ebitda',
            'NHIG': 'wk52_high',
            'NLOW': 'wk52_low',
            'ACFSHR': 'free_cash_flow',
        })
        if save:
            self.save('universe', 'Universe', df_data.to_dict('records'))
        if df:
            # rename column match in table
            return df_data
        return formated_json

    def get_quote(self, ticker, df=False, save=False):
        import math
        quote_url = f'{self.credentials.base_url}Quotes/Quotes.svc/REST/Quotes_1/RetrieveItem_3'
        split = len(ticker)/75
        collected_data =[]
        if split < 1:
            split = math.ceil(split)
        splitting_df = np.array_split(ticker, split)
        for universe in splitting_df:
            ticker = universe.tolist()
            payload = self.retrive_template(ticker, fields=[
                                            'CF_ASK', 'CF_CLOSE', 'CF_BID', 'PCTCHNG', 'CF_HIGH', 'CF_LOW', 'CF_LAST', 'CF_VOLUME', 'TRADE_DATE'])
            response = self.send_request(quote_url, payload, self.auth_headers())

            formated_json_data = self.parse_response(response)
            df_data = pd.DataFrame(formated_json_data).rename(columns={
                'CF_ASK': 'intraday_ask',
                'CF_CLOSE': 'close',
                'CF_BID': 'intraday_bid',
                'CF_HIGH': 'high', 'CF_LOW': 'low',
                'PCTCHNG': 'latest_price_change',
                'TRADE_DATE': 'last_date',
                'CF_VOLUME': 'volume',
                'CF_LAST': 'latest_price'
            })
            df_data['last_date'] = pd.to_datetime(df_data['last_date'])
            collected_data.append(df_data)
        collected_data = pd.concat(collected_data)
        if save:
            self.save('master', 'LatestPrice', collected_data.to_dict('records'))
        if df:
            # rename column match in table
            return collected_data
        return collected_data.to_dict('records')

    @app.task(bind=True,ignore_result=True)
    def save(self, app, model, data):
        from django.apps import apps
        Model = apps.get_model(app, model)
        pk = Model._meta.pk.name
        if isinstance(data, list):
            if isinstance(data[0], dict):
                pass
            else:
                raise Exception('data should be dataframe or list of dict')
        elif isinstance(data, pd.DataFrame):
            data = data.to_dict("records")
        else:
            raise Exception('data should be dataframe or dict')
        key_set = [key for key in data[0].keys()]
        list_obj = []
        for item in data:
            if pk in key_set:
                key_set.remove(pk)
            try:
                key = {pk: item[pk]}
                obj = Model.objects.get(**key)
            except Model.DoesNotExist:
                raise Exception(f'models {item[pk]} does not exist')
            except KeyError:
                raise Exception('no primary key in dict')
            for attr, val in item.items():
                field = obj._meta.get_field(attr)
                if field.one_to_many or field.many_to_many or field.many_to_one or field.one_to_one:
                    attr = f'{attr}_id'
                setattr(obj, attr, val)
            list_obj.append(obj)
        Model.objects.bulk_update(list_obj, key_set)





class RkdStream(RkdData):
    ID =[]

    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs
        super().__init__(*args, **kwargs)
        self.user = self.credentials.username
        self.app_id = self.credentials.extra_data['key']
        self.password = self.credentials.password
        self.position = socket.gethostbyname(socket.gethostname())
        self.token = self.credentials.token
        if not args:
            self.ticker_data=[]
        else:
            self.ticker_data = args[0]
        self.ws_address = "wss://streaming.trkd.thomsonreuters.com/WebSocket"

        self.web_socket_app = websocket.WebSocketApp(self.ws_address, header=['User-Agent: Python'],
                                                     on_message=self.on_message,
                                                     on_error=self.on_error,
                                                     on_close=self.on_close,
                                                     on_open = self.on_open,
                                                     subprotocols=['tr_json2'])
        self.layer = get_channel_layer()

    def stream(self):
        print("Connecting to WebSocket " + self.ws_address + " ...")
        try:

            self.web_socket_app.run_forever()
        except KeyboardInterrupt as e:
            print(f"==========={e}=============")
            self.web_socket_app.close()
        except Exception as e:
            print(f"==========={e}=============")
            self.web_socket_app.close()

    def answer_ping(self, ws, *args, **options):
        ping_req = {"Type": "Pong"}
        ws.send(json.dumps(ping_req))
        print("SENT:")
        print(json.dumps(ping_req, sort_keys=True,
                         indent=2, separators=(',', ':')))

    def write_on_s3(self, message, *args, **options):
        try:
            ticker = message['Key']['Name']
            type_name = message['Type']
            act = message['UpdateType']
            note = json.dumps(message)
            s3 = boto3.client('s3', aws_access_key_id='AKIA2XEOTUNGWEQ43TB6',
                              aws_secret_access_key='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', region_name='ap-east-1')
            # epoch = str(int(datetime.now().timestamp()))
            # s3_file = 'test_dir/'+str(int(epoch)) + ".txt"
            s3_file = "test_dir/"+type_name+"-"+ticker+"-"+act+".txt"
            upload = s3.put_object(
                Body=note, Bucket='droid-v2-logs', Key=s3_file)
        except Exception as e:
            print(f"Error ===== {e}")

    def process_message(self, ws, message_json, *args, **options):
        """ Parse at high level and output JSON of message """
        message_type = message_json['Type']
        # print(f"Message json ====== {message_json}")

        """ check for login response """
        if message_type == "Refresh":
            if 'Domain' in message_json:
                message_domain = message_json['Domain']
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
        # dict_data = {}
        # dict_data["ticker"]=message['Key']['Name']
        # for key, val in message['Fields'].items():
        #     dict_data[change[key]]=val
        #     data.append(dict_data)
        change = {
            'CF_ASK': 'intraday_ask',
            'CF_CLOSE': 'close',
            'CF_BID': 'intraday_bid',
            'CF_HIGH': 'high', 'CF_LOW': 'low',
            'PCTCHNG': 'latest_price_change',
            'TRADE_DATE': 'last_date',
            'CF_VOLUME': 'volume',
            'CF_LAST': 'latest_price'
        }
        if 'PCTCHNG' in message['Fields']:
            message['Fields']['ticker'] = message['Key']['Name']
            data = [message['Fields']]
            df = pd.DataFrame(data).rename(columns=change)
            ticker = df.loc[df['ticker'] == message['Fields']['ticker']]
            print(df)
            # asyncio.run(self.layer.group_send(message['Fields']['ticker'],
            #                                   {
            #     'type': 'broadcastmessage',
            #     'message':  ticker.to_dict('records')
            # }))
            # asyncio.run(self.layer.group_send('topstock',
            #                                   {
            #                                       'type': 'broadcastmessage',
            #                                       'message':  df.to_dict('records')
            #                                   }))
            # self.save.apply_async(
            #     args=('master', 'LatestPrice', df.to_dict('records')),queue='broadcaster')

            # req_payload ={
            #   'type':'function',
            #   'module':'core.djangomodule.crudlib.LatestPrice.save_latest_price',
            #   'data':df.to_dict('records')
            # }
            # send = asyncio.run(celery_publish_msg('#save_latestPrice_channel',df.to_dict('records')))
            self.update_rtdb.apply_async(args=(df.to_dict('records'),),queue='broadcaster')

            del df
            del ticker
        # try:
        #     self.rkd.save('master', 'LatestPrice', data)
        # except Exception as e:
        #     print(e)

    def send_market_price_request(self, ws, *args, **options):
        """ Create and send simple Market Price request """

        mp_req_json = {
            'ID': int(time.time()),
            'Key': {
                'Name': self.ticker_data
            },
            'View': [
                'PCTCHNG',
                'CF_CLOSE',
                'CF_ASK',
                'CF_BID',
                'CF_HIGH',
                'CF_LOW',
                'CF_LAST',
                'CF_VOLUME',
                'TRADE_DATE'
            ]
        }

        ws.send(json.dumps(mp_req_json))
        print("SENT:")
        print(json.dumps(mp_req_json, sort_keys=True,
                         indent=2, separators=(',', ':')))

    def send_login_request(self, ws, *args, **options):
        """ Generate a login request from command line data (or defaults) and send """
        login_json = {'ID': 1, 'Domain': 'Login',
                      'Key': {'NameType': 'AuthnToken', 'Name': '',
                              'Elements': {'AuthenticationToken': '', 'ApplicationId': '', 'Position': ''}
                              }
                      }
        login_json['Key']['Name'] = self.user
        login_json['Key']['Elements']['AuthenticationToken'] = self.token
        login_json['Key']['Elements']['ApplicationId'] = self.app_id
        login_json['Key']['Elements']['Position'] = self.position
        ws.send(json.dumps(login_json))
        print("SENT LOGIN REQUEST:")
        print(json.dumps(login_json, sort_keys=True,
                         indent=2, separators=(',', ':')))

    def on_message(self, ws, message, *args, **options):
        """ Called when message received, parse message into JSON for processing """
        print("")
        print("======== RECEIVED: ========")
        # print(f"ws == {ws}, msg == {message}, args == {args}, option == {options}")
        message_json = json.loads(message)
        # print(json.dumps(message_json, sort_keys=True, indent=2, separators=(',', ':')))

        for singleMsg in message_json:
            self.process_message(ws, singleMsg)

    def on_error(self, ws, error, *args, **options):
        """ Called when websocket error has occurred """
        print(error)
        ws.close()


    def on_close(self, ws, *args, **options):
        # print(super(on_close, self))
        """ Called when websocket is closed """
        print("WebSocket Closed")
        ws.close()

    def on_open(self, ws, *args, **options):
        """ Called when handshake is complete and websocket is open, send login """
        print("WebSocket open!")
        self.send_login_request(ws)

    @app.task(bind=True,ignore_result=True)
    def update_rtdb(self,data):
        data = data[0]
        ticker = data.pop('ticker')
        ref = db.collection('universe').document(ticker)
        ref.set({'price':data},merge=True)
        return ticker
