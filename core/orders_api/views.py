# from alpaca_config import AlpacaAPIConfig
from flask import Flask, redirect, url_for, request, jsonify
# import alpaca_trade_api as tradeapi
from alpaca_trade_api.rest import REST, TimeFrame
import requests
from datetime import datetime, timedelta
import json
import uuid

# untuk alamat callback harus HTTPS dari alpacanya
ngrok = "https://117b8261f440.ngrok.io"
lambda_url = "https://9mxnfs8a8e.execute-api.ap-east-1.amazonaws.com/dev"
url_cb = lambda_url +'/callback'

client_id = '04ae7920b018980616d54fb7320fd923' # test-api
client_secrets = '6092d3adab0521d7b50c63b5fba42ddd94c0f2aa'
random_str = "asjd2y31e2ss309mKjasldnasdn"

clientId = '96029fed617188a2d62c9cc422cd6f4b'
clientSecret = 'ebef248ef6df3b00ee93acf351411fa45487427a'
randomStr = "mlrnfnesadfnesfiunefjsdlfae"

# code = "b584db60-7237-4dfb-8fdd-3a5747702d9"
# token = "Bearer cd0bd73a-eec8-450d-a04f-8c03451725a1 "
# ================== Paper Account Only ======================
key_id = 'PK21TM191A606Z0RX1GR'
secret_key = 'cNwkVAFaEOgcpH83gHstzSafgalCDn2cl70AgVVN'
domain = 'https://paper-api.alpaca.markets'
# domain_data = 'https://data.alpaca.markets/v1'
domain_data = 'wss://data.alpaca.markets'
# ============================================================

Polygon_key = "nk6ee0qwDN_rv3hIfFHnMCyw7PMMGn7Q"
# Alpaca_key = "TKTGO1Z8JRLJDZ0JQWV8"
Alpaca_key = "TKDWWKMLM945BTDAO361"

# =============== client.client_credentials ==================
"""
{
	"domain_for_user_profile"
	"domain_for_stock_data"
	"client_id"
	"client_secret"
	"random_str"	
	"code"
	"apiKey" --> Alpaca_key
	TOKEN
	KEY ID
	SECRET KEY
	CLIENT ID
	CLIENT SECRET
}
"""
# ============================================================

app=Flask(__name__)

def api_init():
	api = REST(key_id, secret_key, base_url=domain)
	return api

# redirect to this page for user authentications
@app.route('/alpaca-auth', methods=['GET'])
def authorizaton():
	url_cb = lambda_url +'/callback'
	url_cb = url_cb.replace(":","%3A")
	url_cb = url_cb.replace("/","%2F")

	if request.args.get('clientId'):
		client_id = request.args.get('clientId')
	
	random_str = uuid.uuid4().hex

	url = "https://app.alpaca.markets/oauth/authorize?response_type=code&client_id=" +client_id+ "&redirect_uri=" +url_cb+ "&state=" +random_str+ "&scope=account:write%20trading"
	req = requests.get(url)
	res = {"url":url}
	return res

@app.route('/callback', methods=['POST','GET'])
def callback():
	code = request.args.get('code') # harunya disimpan, hanya bisa ditukar token sekali <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	if request.method == "GET":
		print("Code >>> ", code)
		res_data_user = {"code":code}	
	else:
		url = "https://api.alpaca.markets/oauth/token"
		data_req = 'grant_type=authorization_code&code='+code+'&client_id='+client_id+'&client_secret='+client_secrets+'&redirect_uri='+url_cb
		header = {
			"Content-Type":"application/x-www-form-urlencoded"
		}
		req = requests.post(url, data=data_req, headers=header)
		res = req.json()
		print("res >>>> ", res) # res['access_token'] harunya disimpan <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

		url = 'https://api.alpaca.markets/oauth/token'
		header = {
		"Authorization":'Bearer '+res['access_token']
		}
		req_data_user = requests.get(url, headers=header)
		res_data_user = req_data_user.json() # res_data_user['id'] disimpan sebagai API key polygon <<<<<<<<<<<<<<<<<<<<<
	return res_data_user

@app.route('/request-token', methods=['GET'])
def request_token():
	code = request.args.get('code') # harunya disimpan, hanya bisa ditukar token sekali <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	if code:
		url = "https://api.alpaca.markets/oauth/token"
		data_req = 'grant_type=authorization_code&code='+code+'&client_id='+client_id+'&client_secret='+client_secrets+'&redirect_uri='+url_cb
		header = {
			"Content-Type":"application/x-www-form-urlencoded"
		}
		req = requests.post(url, data=data_req, headers=header)
		res = req.json()
	else:
		res = {"Messages":"Code credentials needed"}
	return res

@app.route('/request-api-key', methods=['GET'])
def req_polygon_key():
	url = 'https://api.alpaca.markets/oauth/token'
	# url = domain+"/oauth/token"
	header = {
		"Authorization": request.headers.get('Authorization')
	}
	req_data_user = requests.get(url, headers=header)
	res_data_user = req_data_user.json() # res_data_user['id'] disimpan sebagai API key polygon <<<<<<<<<<<<<<<<<<<<<
	return res_data_user
	
@app.route('/recent-trade/<stock>', methods=['GET'])
def recent_trade(stock):
	# url = "https://api.polygon.io/v1/last/stocks/"+stock+"?apiKey="+Alpaca_key
	# url = "https://data.alpaca.markets/v1/last/stocks/"+stock
	url = "https://data.alpaca.markets/v1/last/stocks/"+stock
	header = {
		'APCA-API-KEY-ID': request.headers.get('APCA-API-KEY-ID'),
		'APCA-API-SECRET-KEY': request.headers.get('APCA-API-SECRET-KEY')
	}
	req = requests.get(url, headers=header)
	res = req.json()
	# conditions = conditional_mapping(header)
	# try:
	# 	res['last']['cond1'] = conditions[str(res['last']['cond1'])]
	# 	if res['last']['cond2']:
	# 		res['last']['cond2'] = conditions[str(res['last']['cond2'])]
	# except Exception:
	# 	print(conditions)
	return res


@app.route('/orders', methods=['POST', 'GET'])
def order():
	if request.method == 'GET':
		orders = api_init().list_orders()
		list_orders = []
		for order in orders:
			str_order = {
				'asset_class':order.asset_class,
				'asset_id':order.asset_id,
				'canceled_at':order.canceled_at,
				'client_order_id':order.client_order_id,
				'created_at':order.created_at,
				'expired_at':order.expired_at,
				'extended_hours':order.extended_hours,
				'failed_at':order.failed_at,
				'filled_at':order.filled_at,
				'filled_avg_price':order.filled_avg_price,
				'filled_qty':order.filled_qty,
				'hwm':order.hwm,
				'id':order.id,
				'legs':order.legs,
				'limit_price':order.limit_price,
				'order_class':order.order_class,
				'order_type':order.order_type,
				'qty':order.qty,
				'replaced_at':order.replaced_at,
				'replaced_by':order.replaced_by,
				'replaces':order.replaces,
				'side':order.side,
				'status':order.status,
				'stop_price':order.stop_price,
				'submitted_at':order.submitted_at,
				'symbol':order.symbol,
				'time_in_force':order.time_in_force,
				'trail_percent':order.trail_percent,
				'trail_price':order.trail_price,
				'type':order.type,
				'updated_at':order.updated_at,
			}
			list_orders.append(str_order)
		return {"list_orders":list_orders}
	else:
		payload = {
		"symbol" : request.json['symbol'],
		"qty" : request.json['qty'],
		"side" : "buy",
		"type" : "limit",
		"time_in_force" : "day",
		"limit_price" : request.json['limit_price'],
		"extended_hours" : True,
		"client_order_id" : uuid.uuid4().hex,
		"order_class":"simple", # simple, One Canceled Others(oco), One Trigge Others(oto), bracket
		"take_profit":{
				"limit_price" : request.json['profit_price'],
			},
		"stop_loss":{
			"stop_price" : request.json['stop_price'],
			}
		}
		payload = json.dumps(payload)

		header = {
			"Content-Type":"application/json",
			"Authorization":request.headers.get("Authorization")
			}
		url = domain + '/v2/orders'
		req = requests.post(url, data=payload, headers=header)
		res = req.json() # res['order_id'], res['client_order_id'] harusnya disimpan <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		print(res)
		return res, req.status_code

@app.route('/get-account', methods=['GET'])
def paper_account():
	account = api_init().get_account()

	str_acc = {
		"account_blocked":account.account_blocked,
		"account_number":account.account_number,
		"buying_power":account.buying_power,
		"cash":account.cash,
		"created_at":account.created_at,
		"currency":account.currency,
		"daytrade_count":account.daytrade_count,
		"daytrading_buying_power":account.daytrading_buying_power,
		"equity":account.equity,
		"id":account.id,
		"initial_margin":account.initial_margin,
		"last_equity":account.last_equity,
		"last_maintenance_margin":account.last_maintenance_margin,
		"long_market_value":account.long_market_value,
		"maintenance_margin":account.maintenance_margin,
		"multiplier":account.multiplier,
		"pattern_day_trader":account.pattern_day_trader,
		"portfolio_value":account.portfolio_value,
		"regt_buying_power":account.regt_buying_power,
		"short_market_value":account.short_market_value,
		"shorting_enabled":account.shorting_enabled,
		"sma":account.sma,
		"status":account.status,
		"trade_suspended_by_user":account.trade_suspended_by_user,
		"trading_blocked":account.trading_blocked,
		"transfers_blocked":account.transfers_blocked
		}

	response = {
		"account" : str_acc,
		"positions": api_init().list_positions()
	}
	return response

@app.route('/get-ohlcv/<symbol>', methods=['GET'])
def ohlcv(symbol):
	time_from = request.args.get('timeFrom') 
	time_to = request.args.get('timeTo')
	current_time = datetime.now()
	if time_to >= current_time.strftime("%Y-%m-%d"):
		return {"Messages":False}
	api = REST(key_id, secret_key)
	ohlcv = api.get_bars(str(symbol), TimeFrame.Hour, time_from, time_to, limit=10, adjustment='raw')
	data_list = []
	for data in ohlcv:
		datas = {
			"open":data.o,
			"high":data.h,
			"low":data.l,
			"close":data.c,
			"volume":data.v,
			"timestamp":data.t
		}
		data_list.append(datas)
	return {"Messages":data_list}