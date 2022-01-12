import pandas as pd
import requests
import json
from retry import retry
import time

api_url = "http://quant.loratechai.com:8000"
# api_url = "http://8.210.210.97:8000"

@retry(delay=1)
def get_ai_score_field(tickers, field):
    print(f"API request: {field} from /ai_score")
    query = {'tickers': tickers, 'field': field}
    time.sleep(0.01)
    response = requests.get(f"{api_url}/ai_score/", params=query)
    content = json.loads(response.content)
    print('---> Finished')
    df = pd.DataFrame(content["hits"]).set_index("ticker").rename(columns={"ai_score": field})
    return df

def get_ai_score(tickers, fields):
    ''' get ai_scores from API'''
    df_list = []
    for field in fields:
        df = get_ai_score_field(tickers, field)
        df_list.append(df)
    data = pd.concat(df_list, axis=1).reset_index()
    return data

@retry(delay=1)
def get_ai_score_factor(tickers):
    ''' get positive / negative factors from API '''
    print(f"API request: /ai_score_factor")
    query = {'tickers': tickers}
    response = requests.get(f"{api_url}/ai_score_factor/", params=query)
    content = json.loads(response.content)
    print('---> Finished')
    data = pd.DataFrame(content["hits"])
    return data

@retry(delay=1)
def get_industry():
    ''' get industry code/name from API '''
    print(f"API request: industry_name from /industries")
    response = requests.get(f"{api_url}/industries/?industry_code_length=8")
    content = json.loads(response.content)
    print('---> Finished')
    data = pd.DataFrame(content["industries"])
    return data

@retry(delay=1)
def get_industry_group():
    ''' get industry group code/name from API '''
    print(f"API request: industry_group_name from /industries")
    response = requests.get(f"{api_url}/industries/?industry_code_length=6")
    content = json.loads(response.content)
    print('---> Finished')
    data = pd.DataFrame(content["industries"]).rename(columns={"industry_code": 'industry_group_code',
                                                               "industry_name": "industry_group_name"})
    return data