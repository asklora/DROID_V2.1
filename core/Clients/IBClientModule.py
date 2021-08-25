import requests
import json

class IBClient():
	def __init__(self):
		gateway_host = "https://47.242.9.23"
		gateway_port = f"5000"
		# gateway_host = "https://2.tcp.ngrok.io"
		# gateway_port = f"16229"

		self.gateway_url = f"{gateway_host}:{gateway_port}"
		self.json_headers = {
			"Content-Type":"application/json"
		}

	def request_data(self, method, endpoint, data=None):
		header=self.json_headers
		if method.lower() == 'post':
			if not data:
				return False, "there is no payload attach, please put the payload in"
			print("Request POST")
			req = requests.post(endpoint, headers=header, data=data, verify=False)
		elif method.lower() == 'get':
			req = requests.get(endpoint, verify=False)
		elif method.lower() == 'put':
			req = requests.put(endpoint, data=data, headers=header, verify=False)
		elif method.lower() == 'delete':
			req = requests.delete(endpoint, verify=False)
		else:
			return False, "request method not registered"

		print(f"{req.headers} - {req.status_code} - {req.text}")
		
		if req.status_code == 200:
			res = req.json()
			print(json.dumps(res, sort_keys=True, indent=4))
			return True, res
		else:
			print(f"\n {header} \n {data} - {type(data)}\n {endpoint} \n {method}")
			if req.status_code == 401:
				message = "Session ended unexpectedly"
				return False, message
		return False, None

	def check_status(self):
		url = f"{self.gateway_url}/v1/portal/iserver/auth/status"
		status, response = self.request_data('get', url)
		if not status:
			return "Session did not created"
		else:
			return response

	def get_account(self):
		endpoint = self.gateway_url+"/v1/portal/iserver/accounts"
		status, message = self.request_data(method='GET', endpoint=endpoint)
		if status:
			if "accounts" in message:
				message = message['accounts']
				print(message)
				return message
		"""backup plan when request accounts get == {} then we fetch within subaccounts and pull their id"""
		porto_endpoint = f"{self.gateway_url}/v1/api/portfolio/subaccounts"
		status, message = self.request_data(method='GET', endpoint=porto_endpoint)
		if status:
			messages = []
			for msg in message:
				messages.append(msg['id'])
		print(messages)
		return messages

	def account_legder(self,account_id=None):
		if not account_id:
			account_id = self.get_account() 
			if "All" in account_id:
				account_id.remove('All')
			if f'95% group' in account_id:
				account_id.remove(f'95% group')
		if isinstance(account_id, str):
			url = f"{self.gateway_url}/v1/api/portfolio/{account_id}/ledger"
			status, response = self.request_data('get', url)
		else:
			response = []
			status = True
			for account in account_id:
				url = f"{self.gateway_url}/v1/api/portfolio/{account}/ledger"
				status, resp = self.request_data('get', url)
				response.append(resp)
				if not status:
					break

		if status:
			result = {}
			if isinstance(response, list):
				print("it's a list")
				for resp in response:
					for key, val in resp.items():
						if key in result:
							result[key] += val['cashbalance']
						else:
							result[key] = val['cashbalance']
			else:
				for key, val in response.items():
					result[key] = val['cashbalance']
			print(f"------ result ----- \n {json.dumps(result, indent=4)}")
			return result
		else:
			return "Failed to fetch data"

	def portofolio_account(self):
		url = f"{self.gateway_url}/v1/api/portfolio/accounts"
		status, response = self.request_data("get", url)
		if status:
			return response
		else:
			return "Failed to fetch data"

	"""
	def find_contract(self, ticker_id):
		endpoint = self.gateway_url+"/v1/portal/iserver/secdef/search"
		#  as stephen says we traded Stock only, so i set it to STK in secType
		payload = {
			"symbol":ticker_id,
			"name":False,
			"secType":"STK"
		}
		payload = json.dumps(payload)
		status, message = self.request_data(method='post', endpoint=endpoint, data=payload)
		print(f"Messages >> {message}")
		if status:
			if isinstance(message, list):
				messages = []
				for res in message:
					msg = {
						"contract_id":res['conid'],
						"company_name":res['companyName'],
						"company_header":res['companyHeader'],
						"description":res['description']
					}
					messages.append(msg)
			return messages
		else:
			return message
		"""

	def find_contract(self, ticker_id):
		url = f"{self.gateway_url}/v1/api/trsrv/stocks?symbols={ticker_id}"
		status, response = self.request_data('get',url)
		print(response)
		resp = None
		if status:
			contracts = response[ticker_id]
			for contract in contracts:
				if contract['assetClass'] == "STK":
					for con in contract['contracts']:
						if con['isUS']:
							resp = con
			# print(f"resp >> {resp}")
			return resp
		else:
			return None


	def limit_order(self, account_id, con_order_id, price, qty, side, conID=None, ticker=None):
		if account_id not in self.get_account():
			return "Account ID is not registered"
		else:
			if not conID:
				if not ticker:
					return "please submit your ticker symbol or contract id"
				list_of_contract = self.find_contract(ticker)
				if not list_of_contract:
					return "Failed to fetch data"
				conID = list_of_contract['conid']

		endpoint = f'{self.gateway_url}/v1/api/iserver/account/{account_id}/order'
		payload = {
			"conid": conID,
			"secType": f"{conID}:STK",
			"cOID": con_order_id,
			"orderType": "LMT",
			"price": price,
			"side": side,
			"quantity": qty,
			"tif": "DAY"
		}
		payload = json.dumps(payload)
		status, message = self.request_data(method='post', endpoint=endpoint, data=payload)
		print(f"{status} - {message}")
		if not status:
			return f"Failed to order"
		else:
			return message

	def market_order(self, qty, account_id, con_order_id, side, conID=None, ticker=None):
		list_of_account = self.get_account()
		if account_id not in list_of_account:
			msg = "Account ID is not registered"
			print(msg)
			return msg
		else:
			if not conID:
				if not ticker:
					msg = "please submit your ticker symbol or contract id"
					return msg
				list_of_contract = self.find_contract(ticker)
				if not list_of_contract:
					msg = "Failed to fetch data"
					return msg
				conID = list_of_contract['conid']

				"""==== this is just debug for AAPL stock, comment out this pat and you should choose the conrtact id ===="""
				# for con in list_of_contract:
				# 	if con['description'] == 'NASDAQ': 
				# 		conID = con['contract_id']
				""" ======================================================================================================"""
				""" if you comment out the code above, you should not comment out the return below""" 
				# return list_of_contract

			endpoint = f'{self.gateway_url}/v1/api/iserver/account/{account_id}/order'
			if isinstance(qty, int):
				print("----------------------- initiate payload -----------------------")
				payload = {
						"conid": conID,
						"secType": f"{conID}:STK",
						"cOID": con_order_id,
						"orderType": "MKT",
						"side": side,
						"quantity": qty,
						"tif": "DAY"
					}
				payload = json.dumps(payload)
				status, message = self.request_data(method='post', endpoint=endpoint, data=payload)
				if status:
					print("Success")
					print(message)
				else:
					print("Failed")
				return message
			else:
				msg = f"QTY must in integer not in {type(qty)}"
				return msg

	def order_status(self, orderId):
		url = f"{self.gateway_url}/v1/api/iserver/account/order/status/{orderId}"
		status, response = self.request_data("get", url)
		if status:
			return response
		else:
			return "Failed to fetch data"

	def cancel_order(self, account_id, orderId):
		url = f"{self.gateway_url}/v1/api/iserver/account/{account_id}/order/{orderId}"
		status, response = self.request_data('delete', url)
		if status:
			return response
		else:
			return "Failed to fetch data"

	def get_position(self, account_id, conID):
		url = f"{self.gateway_url}/v1/api/portfolio/{account_id}/positions/{conID}"
		stat,res =self.request_data('get', url)
		print(stat,res)
		if stat:
			return res
		else:
			return "Failed to fetch data"



# if __name__=="__main__":
# 	c = Client()
# 	c.find_contract('FB')