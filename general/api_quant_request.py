import pandas as pd
import requests
import json

api_url = "http://quant.loratechai.com:8000"
# api_url = "http://192.168.49.2:31493"

def get_ai_score(tickers, field='ai_score'):
    query = {'tickers': tickers, 'field': field}
    response = requests.get(f"{api_url}/ai_score/", params=query)
    content = json.loads(response.content)
    print(type(content), content)
    return content

def get_ai_score_factor(tickers):
    query = {'tickers': tickers}
    response = requests.get(f"{api_url}/ai_score_factor/", params=query)
    content = json.loads(response.content)
    print(type(content), content)
    return content

def get_industry():
    response = requests.get(f"{api_url}/industry/")
    content = json.loads(response.content)
    print(type(content), content)
    data = pd.DataFrame(content)
    data.index.name = 'industry_code'
    return data.reset_index()

def get_industry_group():
    response = requests.get(f"{api_url}/industry_group/")
    content = json.loads(response.content)
    print(type(content), content)
    data = pd.DataFrame(content)
    data.index.name = 'industry_group_code'
    return data.reset_index()