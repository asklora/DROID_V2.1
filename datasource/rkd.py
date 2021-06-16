import requests
import json
import pandas as pd
from requests.api import head
from core.services.models import ThirdpartyCredentials
from core.djangomodule.models import DataFrameModels
import sys
import logging
logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S',level=logging.INFO)



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

    
    def send_request(self, url,payload,headers):
        result=None

        try:
            result = requests.post(url,data=json.dumps(payload),headers=headers)
            if result.status_code != 200:
                
                print('Request fail')
                print(f'request url :  {url}')
                print(f'request header :  {headers}')
                print(f'request payload :  {payload}')
                print(f'response status {result.status_code}')
                if result.status_code == 500: ## if username or password or appid is wrong
                    print('Error: %s'%(result.json()))
                    return None
        except requests.exceptions.RequestException as e:
            print(e)
            sys.exit()
        return result.json()
    
    def get_token(self):
        authenMsg = {   
            'CreateServiceToken_Request_1':{ 
                                            'ApplicationID':self.credentials.extra_data['key'], 
                                            'Username':self.credentials.username,
                                            'Password':self.credentials.password 
                                            }
                    }
        authenURL = f'{self.credentials.base_url}TokenManagement/TokenManagement.svc/REST/Anonymous/TokenManagement_1/CreateServiceToken_1'
        print('logged you in')
        print('requesting new token')
        response = self.send_request(authenURL, authenMsg,self.headers)
        self.credentials.token = response['CreateServiceToken_Response_1']['Token']
        self.credentials.save()
        print('new token saved')
        return self.credentials.token

    
    @property
    def is_valid_token(self):

        payload ={
                    "ValidateToken_Request_1": {
                    "ApplicationID":self.credentials.extra_data['key'],
                    "Token": self.credentials.token
                    }
                }
        headers=self.auth_headers()
        validate_url = f'{self.credentials.base_url}TokenManagement/TokenManagement.svc/REST/TokenManagement_1/ValidateToken_1'
        retry=0
        while True:
            response = self.send_request(validate_url,payload,headers)
            if response:
                break
            else:
                retry += 1
                self.get_token()
                payload['ValidateToken_Request_1']['Token'] =self.credentials.token
                headers['X-Trkd-Auth-Token'] = self.credentials.token
                response = self.send_request(validate_url,payload,headers)
            if retry >=3:
                raise ConnectionError(f'failed after, retry {retry} times')


        return response['ValidateToken_Response_1']['Valid']

    def auth_headers(self):
        headers = self.headers
        headers['X-Trkd-Auth-ApplicationID'] = self.credentials.extra_data['key']
        headers['X-Trkd-Auth-Token'] = self.credentials.token
        return headers

    def get_quote(self,ticker,df=False):
        quote_url = f'{self.credentials.base_url}Quotes/Quotes.svc/REST/Quotes_1/RetrieveItem_3'
        payload ={
                    "RetrieveItem_Request_3": {
                        "ItemRequest": [
                            {
                                "Fields": "CF_ASK: CF_BID: CF_CLOSE",
                                "RequestKey": [
                                
                                ],
                                "Scope": "List"
                            }
                        ],
                        "TrimResponse": True,
                        "IncludeChildItemQoS": False
                    }
                    }
        if isinstance(ticker,str):
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

        
        
        response = self.send_request(quote_url,payload,self.auth_headers())
        json_data = response['RetrieveItem_Response_3']['ItemResponse'][0]['Item']
        formated_json_data=[]
        for index,item in enumerate(json_data):
            ticker = item['RequestKey']['Name']
            formated_json_data.append({'ticker':ticker})
            for f in item['Fields']['F']:
                field =f['n']
                val =f['Value']
                formated_json_data[index].update({field:val})
        if df:
            df_data = pd.DataFrame(formated_json_data).rename(columns={'CF_ASK':'intraday_ask','CF_CLOSE':'close','CF_BID':'intraday_bid'})
            df_django = DataFrameModels(df_data)
            import gc
            del df_data
            gc.collect()
            # rename column match in table
            return df_django
        return formated_json_data
        

    

    

