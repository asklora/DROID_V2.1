import requests
import json

class Client():

	def __init__(self):
		# gateway_host = f"https://47.242.9.23"
		gateway_host = f"https://localhost"
		gateway_port = f"5000"
		self.gateway_url = f"{gateway_host}:{gateway_port}"
		self.json_headers = {
			"Content-Type":"application/json"
		}

	def request_data(self, method, endpoint, data=None):
		header=self.json_headers
		if method.lower() == 'post':
			if not data:
				return False, "there is no payload attach, please put the payload in"
			req = requests.post(endpoint, headers=header, data=data, verify=False)
		elif method.lower() == 'get':
			req = requests.get(endpoint, verify=False)

		
		# print(f"{req.headers} - {req.status_code} - {req.text}")
		
		if req.status_code == 200:
			res = req.json()
			return True, res
		else:
			print(f"\n {header} \n {data} - {type(data)}\n {endpoint} \n {method}")
			if req.status_code == 401:
				print("Session ended unexpectedly")
		return False, None

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
			

	def find_contract(self, ticker_id):
		endpoint = self.gateway_url+"/v1/portal/iserver/secdef/search"
		""" as stephen says we traded Stock only, so i set it to STK in secType"""
		payload = {
			"symbol":ticker_id,
			"name":False,
			"secType":"STK"
		}
		payload = json.dumps(payload)
		status, message = self.request_data(method='post', endpoint=endpoint, data=payload)
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
			print(messages)
			return messages
		else:
			return message


	def market_order(self, qty, account_id, con_order_id, conID=None, ticker=None):
		if account_id not in self.get_account():
			return "Account ID is not registered"
		else:
			if not conID:
				if not ticker:
					return "please submit your ticker symbol or contract id"
				list_of_contract = self.find_contract(ticker)
				"""==== this is just debug for AAPL stock, comment out this pat and you should choose the conrtact id ===="""
				for con in list_of_contract:
					if con['description'] == 'NASDAQ': 
						conID = con['contract_id']
				""" ======================================================================================================"""
				""" if you comment out the code above, you should not comment out the return below""" 
				# return list_of_contract

			endpoint = f'{self.gateway_url}/v1/api/iserver/account/{account_id}/order'
			if isinstance(qty, int):
				payload = {
						"conid": conID,
						"secType": f"{conID}:STK",
						"cOID": con_order_id,
						"orderType": "MKT",
						"side": "BUY",
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
				return f"QTY must in integer not in {type(qty)}"
	
	def get_position(self,accountId,conid):
		stat,res =self.request_data('get',f'https://localhost:5000/v1/api/portfolio/{accountId}/positions/{conid}')
		print(res)


# if __name__=="__main__":
# 	c = Client()
# 	c.find_contract('FB')