import requests
import json
import pandas as pd



class Rkd:
    url = 'https://api.rkd.refinitiv.com/api/'
    appid = 'rkddemoappwm'
    password = 's9d5z48ht'
    username = 'rkd-demo-wm@refinitiv.com'



    def __init__(self):
        super().__init__()

    
    def token(self):
        authenMsg = {'CreateServiceToken_Request_1': { 'ApplicationID':self.appid, 'Username':self.username,'Password':self.password }}
        authenURL = f'{self.url}TokenManagement/TokenManagement.svc/REST/Anonymous/TokenManagement_1/CreateServiceToken_1'
        headers = {'content-type': 'application/json;charset=utf-8'}
        print(authenMsg)
        print(authenURL)
        response = requests.post(authenURL, data=json.dumps(authenMsg), headers=headers)
        print(response.status_code,response.text)