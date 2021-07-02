import requests
import json
import pandas as pd
from requests.api import head
from core.services.models import ThirdpartyCredentials
import sys
import logging
logging.basicConfig(format='%(asctime)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')
logging.getLogger().setLevel(logging.INFO)

import aiohttp
import asyncio




    


class Rkd:
    token = None

    def __init__(self):
        self.credentials = ThirdpartyCredentials.objects.get(services='RKD')
        self.headers = {'content-type': 'application/json;charset=utf-8'}
        if self.is_valid_token:
            logging.info('valid token')
            logging.info('using existing token')
            self.token = self.credentials.token
        else:
            self.token = self.get_token()
    
    async def gather_request(self,url, payload, headers):
        async with aiohttp.ClientSession(headers=headers) as session:

            responses =[]
            for data in payload:
                responses.append(asyncio.ensure_future(self.async_send_request(session,url, data, headers)))

            original_responses = await asyncio.gather(*responses)
            return original_responses
     
        
    
    async def async_send_request(self, session,url, payload, headers):
         async with session.post(url,data=json.dumps(payload)) as resp:
            response = await resp.json()
            status = resp.status
            
            if status == 200:
                return response
            else:
                response = {
                    "ticker":payload["GetRatiosReports_Request_1"]["companyId"],
                    "Error":response,
                    "AREVPS":None,
                    "MKTCAP":None,
                    "APEEXCLXOR":None,
                    "ProjPE":None,
                    "APRICE2BK":None,
                    "EV":None,
                    "AEBITD":None,
                    "NHIG":None,
                    "NLOW":None,
                    "ACFSHR":None,
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

    def __init__(self):
        super().__init__()

    
    
        
    
    
    
    
    def retrive_template(self, ticker, scope="List", fields=''):

        if scope == 'List':
            if not fields or fields == '':
                raise ValueError('fields keyword argument must set if scope is list')
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

    def get_snapshot(self, ticker,save=False,df=False):
        snapshot_url = f'{self.credentials.base_url}Fundamentals/Fundamentals.svc/REST/Fundamentals_1/GetRatiosReports_1'
        list_formated_json =[]
        if isinstance(ticker,list):
            list_payload = []
            for tic in ticker:
                payload = {
            "GetRatiosReports_Request_1": {
                "companyId": tic,
                "companyIdType": "RIC"
                        }
                        }
                list_payload.append(payload)
            responses = asyncio.run(self.gather_request(snapshot_url,list_payload,self.auth_headers()))
            # print(responses)
            # with open('data.json', 'w') as fp:
            #     json.dump(responses, fp,  indent=4)
            # sys.exit()
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
                            'EV',
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
                            formated_json[group_item['FieldName']] = group_item['Value'][0]['Value']
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
                    'EV',
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
                    formated_json[group_item['FieldName']] = group_item['Value'][0]['Value']
            list_formated_json.append(formated_json)
        df_data = pd.DataFrame(list_formated_json).rename(columns={
            'AREVPS':'revenue_per_share',
            'MKTCAP':'market_cap',
            'APEEXCLXOR':'pe_ratio',
            'ProjPE':'pe_forecast',
            'APRICE2BK':'pb',
            'EV':'ev',
            'AEBITD':'ebitda',
            'NHIG':'wk52_high',
            'NLOW':'wk52_low',
            'ACFSHR':'free_cash_flow',
            })
        if save:
            self.save('universe', 'Universe', df_data.to_dict('records'))
        if df:
            # rename column match in table
            return df_data
        return formated_json

    def get_quote(self, ticker, df=False, save=False):
        quote_url = f'{self.credentials.base_url}Quotes/Quotes.svc/REST/Quotes_1/RetrieveItem_3'
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
            'CF_VOLUME':'volume',
            'CF_LAST':'latest_price'
            })
        if save:
            self.save('master', 'LatestPrice', df_data.to_dict('records'))
        if df:
            # rename column match in table
            return df_data
        return formated_json_data

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
        Model.objects.bulk_update(list_obj, key_set, batch_size=500)
