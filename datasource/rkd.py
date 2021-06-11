import requests
import json
import pandas as pd
from requests.api import head
from core.services.models import ThirdpartyCredentials
import sys


class Rkd:
    token = None

    def __init__(self):
        self.credentials = ThirdpartyCredentials.objects.get(services='RKD')
        self.headers = {'content-type': 'application/json;charset=utf-8'}
        if self.is_valid_token:
            print('valid token')
            print('using existing token')
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
                raise ValueError('failed')


        return response['ValidateToken_Response_1']['Valid']

    def auth_headers(self):
        headers = self.headers
        headers['X-Trkd-Auth-ApplicationID'] = self.credentials.extra_data['key']
        headers['X-Trkd-Auth-Token'] = self.credentials.token
        return headers

    def get_quote(self,ticker):
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
        for item in json_data:
            print(item['RequestKey']['Name'])
        

    

    
