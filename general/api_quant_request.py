import pandas as pd
import requests
import json
from retry import retry
import time

# api_url = "http://quant.loratechai.com:8000"
api_url = "http://8.210.201.22:8000"

@retry(delay=1)
def get_ai_score(tickers, fields):
    print(f"API request: {fields} from /ai_score")
    query = {'tickers': tickers, 'fields': fields, "weeks_to_expire": [4]}
    response = requests.get(f"{api_url}/ai_score/", params=query)
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
    query = {'tickers': tickers, "weeks_to_expire": 4}      # ai_score using 4w
    response = requests.get(f"{api_url}/ai_score_factor/", params=query)
    content = json.loads(response.content)
    print('---> Finished')
    data = pd.DataFrame(content["hits"])
    for i in ["positive", "negative"]:
        data[f"{i}_factors"] = pd.json_normalize(data["details"], max_level=1)[f"currency.{i}"]
        data[f"{i}_factors"] = [list(i.keys()) if i!="NA" else [] for i in data[f"{i}_factors"]]
    return data.drop(columns=["details"])

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

if __name__ == '__main__':

    tickers = ["AAPL.O", "TSLA.O", ".SPX"]
    get_ai_score(tickers, fields=["ai_score", "ai_score2"])
    get_ai_score_factor(tickers)
    get_industry()
    get_industry_group()