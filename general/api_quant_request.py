import os
from dotenv import load_dotenv
from environs import Env
import pandas as pd
import requests
import json
from retry import retry
import time

api_url = "http://quant.loratechai.com:8000"
# api_url = "http://8.210.201.22:8000"
# api_url = "http://192.168.49.2:31260"

env = Env()
load_dotenv()
headers = {"x-secret-key": os.getenv("API_AUTH")}
print(headers)

@retry(delay=1)
def get_ai_score(tickers, fields):
    print(f"API request: {fields} from /ai_score")
    query = {'tickers': tickers, 'fields': fields, "weeks_to_expire": [8]}
    response = requests.get(f"{api_url}/ai_score/", params=query, headers=headers)
    content = json.loads(response.content)
    print('---> Finished')
    df = pd.DataFrame(content["hits"])
    df = df.drop(columns=["trading_day", "weeks_to_expire"])
    df = df.pivot(index=['ticker'], columns=['field'], values='value')
    return df

@retry(delay=1)
def get_ai_score_factor(tickers):
    ''' get positive / negative factors from API '''
    print(f"API request: /ai_score_factor")
    query = {'tickers': tickers, "weeks_to_expire": 8}      # ai_score using 4w
    response = requests.get(f"{api_url}/ai_score_factor/", params=query, headers=headers)
    content = json.loads(response.content)
    print('---> Finished')
    data = pd.DataFrame(content["hits"])
    for pn in ["positive", "negative"]:
        data[f"{pn}_factors"] = pd.json_normalize(data["details"], max_level=1)[f"currency.{pn}"].fillna("NA")
        data[f"{pn}_factors"] = [list(i.keys()) if ((i != "NA") & (len(i) > 0)) else [] for i in data[f"{pn}_factors"]]
    return data.drop(columns=["details"])

@retry(delay=1)
def get_industry():
    """ get industry code/name from API """
    print(f"API request: industry_name from /industries")
    response = requests.get(f"{api_url}/industries/?industry_code_length=8", headers=headers)
    content = json.loads(response.content)
    print('---> Finished')
    data = pd.DataFrame(content["industries"])
    return data

@retry(delay=1)
def get_industry_group():
    ''' get industry group code/name from API '''
    print(f"API request: industry_group_name from /industries")
    response = requests.get(f"{api_url}/industries/?industry_code_length=6", headers=headers)
    content = json.loads(response.content)
    print('---> Finished')
    data = pd.DataFrame(content["industries"]).rename(columns={"industry_code": 'industry_group_code',
                                                               "industry_name": "industry_group_name"})
    return data

if __name__ == '__main__':

    response = requests.get(f"{api_url}/universe/?field=ticker", headers=headers)
    tickers = json.loads(response.content)
    tickers = pd.DataFrame(tickers["hits"])['ticker'].to_list()
    # print(get_ai_score(tickers, fields=["ai_score", "ai_score2"]))
    print(get_ai_score_factor(tickers))
    # print(get_industry())
    # print(get_industry_group())