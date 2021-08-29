import json
import time
import queue

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.order import (OrderComboLeg, Order)
from ibapi.contract import Contract
import random
from threading import Thread,Lock as LockThread
import logging



logging.basicConfig(format="%(asctime)s - %(message)s",
                    datefmt="%d-%b-%y %H:%M:%S")
logging.getLogger().setLevel(logging.INFO)




class ApiPool(type):
	__lock = LockThread()
	__instance =None
	
	def __new__(cls,*args, **kwargs):
		with cls.__lock:
			if not cls.__instance:
				cls.__instance =super(ApiPool, cls).__new__(cls,*args, **kwargs)
		return cls.__instance


class ResponseHandler:
	def __init__(self):
		pass

	@staticmethod
	def handler(msg):
		logging.info(msg)


class TestApp(EWrapper, EClient):
	def __init__(self,*args, **kwargs):
		EClient.__init__(self, self)
		self.nextId=random.randint(0,255)
	
	def connect(self, host, port, clientId):
		super().connect(host, port, clientId)
		# self.result_queue = queue

	def error(self, reqId, errorCode, errorString):
		super().error(reqId, errorCode, errorString)
		print("Error: ", reqId, " ", errorCode, " ", errorString)

	def reqIds(self, numIds, queue=None):
		super().reqIds(numIds)
		if queue:
			self.queue_id = queue

	def nextValidId(self, orderId:int):
		super().nextValidId(orderId)
		print(f"OrderId >> {orderId}")
		self.nextId = orderId+1

		# if isinstance(orderId, int):
		# 	nextId = orderId+1
		# else:
		# 	nextId = int(orderId)+1

		# if self.queue_id:
		# 	messages = {}
		# 	messages['nextValidId'] = nextId
		# 	print(f"Next >> {nextId}")
		# 	self.queue_id.put(messages)
		# 	self.disconnect()

	def reqAccountSummary(self, reqId, groupName, tags):
		super().reqAccountSummary(reqId, groupName, tags)
		self.messages = {}
		self.messages['accountSummary'] = []

	def accountSummary(self, reqId, account, tag, value, currency):
		super().accountSummary(reqId, account, tag, value, currency)
		self.messages["accountSummary"].append({account:value})
		print(f"reqId:{reqId} \n account:{account} \n tag:{tag} \n value:{value} \n currency:{currency}")

	def accountSummaryEnd(self, reqId):
		super().accountSummaryEnd(reqId)
		# self.result_queue.put(self.messages)
		self.messages = {}
		# self.disconnect()

	def contractDetails(self, reqId, contractDetails):
		super().contractDetails(reqId, contractDetails)
		# print("contractDetails: ", reqId, " ", contractDetails)
		print(f"contractDetails: \n reqId : {reqId} \n Contract : \
			marketName : {contractDetails.marketName} \n \
			minTick : {contractDetails.minTick} \n \
			orderTypes : {contractDetails.orderTypes} \n \
			validExchanges : {contractDetails.validExchanges} \n \
			priceMagnifier : {contractDetails.priceMagnifier} \n \
			underConId : {contractDetails.underConId} \n \
			longName : {contractDetails.longName} \n \
			contractMonth : {contractDetails.contractMonth} \n \
			industry : {contractDetails.industry} \n \
			category : {contractDetails.category} \n \
			subcategory : {contractDetails.subcategory} \n \
			timeZoneId : {contractDetails.timeZoneId} \n \
			tradingHours : {contractDetails.tradingHours} \n \
			liquidHours : {contractDetails.liquidHours} \n \
			evRule : {contractDetails.evRule} \n \
			evMultiplier : {contractDetails.evMultiplier} \n \
			mdSizeMultiplier : {contractDetails.mdSizeMultiplier} \n \
			underSymbol : {contractDetails.underSymbol} \n \
			underSecType : {contractDetails.underSecType} \n \
			marketRuleIds : {contractDetails.marketRuleIds} \n \
			aggGroup : {contractDetails.aggGroup} \n \
			secIdList : {contractDetails.secIdList} \n \
			realExpirationDate : {contractDetails.realExpirationDate} \n \
			cusip : {contractDetails.cusip} \n \
			ratings : {contractDetails.ratings} \n \
			descAppend : {contractDetails.descAppend} \n \
			bondType : {contractDetails.bondType} \n \
			couponType : {contractDetails.couponType} \n \
			callable : {contractDetails.callable} \n \
			putable : {contractDetails.putable} \n \
			coupon : {contractDetails.coupon} \n \
			convertible : {contractDetails.convertible} \n \
			maturity : {contractDetails.maturity} \n \
			issueDate : {contractDetails.issueDate} \n \
			nextOptionDate : {contractDetails.nextOptionDate} \n \
			nextOptionType : {contractDetails.nextOptionType} \n \
			nextOptionPartial : {contractDetails.nextOptionPartial} \n \
			notes : {contractDetails.notes} \n \
			type of contract : {type(contractDetails)}")

	def contractDetailsEnd(self, reqId):
		super().contractDetailsEnd(reqId)
		# self.disconnect()

	def symbolSamples(self, reqId, contractDescriptions):
		super().symbolSamples(reqId, contractDescriptions)
		print("Symbol Samples. Request Id: ", reqId)
		messages = {}
		messages['symbolSamples'] = []
		contract = None

		for contractDescription in contractDescriptions:
			derivSecTypes = ""
			for derivSecType in contractDescription.derivativeSecTypes:
				derivSecTypes += derivSecType
				derivSecTypes += " "
			
			contract = {
				"conId":contractDescription.contract.conId,
				"symbol":contractDescription.contract.symbol,
				"secType":contractDescription.contract.secType,
				"primaryExchange":contractDescription.contract.primaryExchange,
				"currency":contractDescription.contract.currency, 
				"contract":contractDescription.contract
				# "derivSecTypes":derivSecTypes
			}
			# print(contract)
			messages['symbolSamples'].append(contract)			
		# 	self.result_queue.put(messages)
		# self.disconnect()
		ResponseHandler.handler(messages)

	def placeOrder(self, orderId , contract, order, queue=None):
		super().placeOrder(orderId, contract, order)
		if queue:
			self.queue_order = queue

	def openOrder(self, orderId, contract, order, orderState):
		super().openOrder(orderId, contract, order, orderState)
		response = {
			"orderId":orderId,
			"contract":contract,
			"order":order,
			"orderState":orderState,
		}

		# if self.queue_order:
		# 	message = {}
		# 	message["openOrder"] = response
		# 	self.queue_order.put(message)
		# else:
		# 	pass
		print(f"openOrder >> {response}")

	def orderStatus(self, orderId , status, filled,remaining, avgFillPrice, permId,parentId, lastFillPrice, clientId,whyHeld, mktCapPrice):
		super().orderStatus(orderId , status, filled,remaining, avgFillPrice, permId,parentId, lastFillPrice, clientId,whyHeld, mktCapPrice)
		response = {
			"orderId":orderId,
			"status":status,
			"filled":filled,
			"remaining":remaining,
			"avgFillPrice":avgFillPrice,
			"permId":permId,
			"lastFillPrice":lastFillPrice,
			"clientId":clientId,
			"whyHeld":whyHeld,
			"mktCapPrice":mktCapPrice
		}
		print(f"orderStatus >> {response} \n")
		# if self.queue_order:
		# 	if not self.queue_order.empty():
		# 		res = self.queue_order.get(timeout=0.5)
		# 		print(f"res in orderStatus >> \n {res}")
		# 		res['orderStatus'] = response
		# 		self.queue_order.put(res)
		# 	else:
		# 		message = {}
		# 		message ['orderStatus'] = response
		# 		self.queue_order.put(message)
		# 	self.disconnect()

class TwsContract(Contract):
	def __init__(self):
		super().__init__()

	@classmethod
	def make_contract(cls,*args,**kwargs):
		contract =cls()
		for key,value in kwargs.items():
			if hasattr(contract,key):
				setattr(contract,key,value)
			else:
				raise ValueError(f"object doesnt have {key} properties")
		return contract
			

class TwsApi(metaclass=ApiPool):
	tws_contract = TwsContract


	def __init__(self,Id,*args, **kwargs):
		self.Id =Id
		self.server= TestApp()
		
	
	def run(self):
		if not self.server.isConnected():
			self.server.connect("localhost",4002,self.Id)
			self.app_thread = Thread(target=self.server.run,daemon=True)
			return self.app_thread.start()

	def stop(self,signal=None,frame=None):
		self.server.disconnect()
		self.server.reset()
		
		if self.app_thread.isAlive():
			self.app_thread.join()
		if signal:
			import sys
			sys.exit(1)

	def get_id(self):
		return self.server.reqIds(0)

	def get_contract(self,symbol:str,identifier:str='RIC'):
		ids = self.server.nextId
		self.server.reqMatchingSymbols(ids,symbol)
		


class TWSClient:

	def __init__(self):
		self.host = "localhost"
		self.port = 4002
		self.result_queue = queue.Queue()
		# self.result_queue = result_queue

	def get_valid_id(self, numIds):
		app = TestApp()
		print("requesting id ...")
		id_queue = queue.Queue()
		app.connect(self.host, self.port, 0, self.result_queue)
		app.reqIds(numIds=numIds, queue=id_queue)
		app.run()

		nexId = None
		while True:
			if id_queue.empty():
				pass
			else:
				nexId = id_queue.get(timeout=0.5)
				if "nextValidId" in nexId:
					nexId  = nexId["nextValidId"]
					print(f"Next valid ID == {nexId}")
					break
				else:
					print(f"Next valid ID == {nexId}")
					pass
		return nexId


	def get_contracts(self, ticker_symbol):
		# ids = 2
		nexId = 2
		# nexId = self.get_valid_id(ids)
		app = TestApp()
		app.connect(self.host, self.port, 0, self.result_queue)
		print("requesting symbols ...")
		app.reqMatchingSymbols(nexId, ticker_symbol)
		print("running WS")
		app.run()

		res = None
		# contract = None
		contract = Contract()
		res_detail = None
	
		while True:
			if self.result_queue.empty():
				print("Queue Empty")
				pass
			else:
				res = self.result_queue.get(timeout=0.5)
				if "symbolSamples" in res:
					res = res['symbolSamples']
					print(f"data in get_contracts >> {res}")
					break
				else:
					print(f"data in get_contracts >> {res}")
					pass
			
		for data in res:
			if data['symbol'] == ticker_symbol:
				if data['secType'] == 'STK':
					if data['currency'] == 'USD':
						contract.symbol = ticker_symbol
						contract.secType = data['secType']
						contract.exchange = "SMART"
						contract.currency = data['currency']
						if "NASDAQ" in data['primaryExchange']:
							contract.primaryExchange = "NASDAQ"
						else:
							contract.primaryExchange = data['primaryExchange']
						res_detail = data

		print(f"response = {res_detail}")
		return contract
	"""
	def find_contract_detail(self, ticker_symbol):
		ids = 3
		nexId = self.get_valid_id(ids)
		contract = self.get_contracts(ticker_symbol)
		app = TestApp()
		app.connect(self.host, self.port, 0, self.result_queue)
		print("requesting contract ...")
		app.reqContractDetails(nexId, contract)
		print("running WS")
		app.run()
	"""
	def request_account(self):
		app = TestApp()
		app.connect(self.host, self.port, 0, self.result_queue)
		app.reqAccountSummary(6, "All", "AccountType")
		app.run()

		# while True:
		# 	if self.result_queue.empty():
		# 		print("queue empty")
		# 		pass
		# 	else:
		# 		response = self.result_queue.get(timeout=0.2)
		# 		print(f"result of func >> {response}")
		# 		break

	def order(self, reqId, ticker_symbol, side, qty, type_order, price=None):
		order_queue = queue.Queue()

		contract = self.get_contracts(ticker_symbol)
		ids = self.get_valid_id(reqId)
		print(f"id for orders >> {ids}")

		app = TestApp()
		app.connect(self.host, self.port, 0, self.result_queue)
		print("Preparing Order ...")
		order = Order()
		order.action = side
		order.totalQuantity = qty
		order.orderType = type_order
		order.transmit = True
		order.account = "DU2898617"

		if type_order == "LMT":
			order.lmtPrice = price

		print("Placing Order ...")
		app.placeOrder(ids, contract, order, order_queue)
		app.run()
		res  = None

		while True:
			if order_queue.empty():
				print("empty")
				pass
			else:
				res = order_queue.get(timeout=0.2)
				print(f"response order >> \n {res}")
				break
		return res




# if __name__=="__main__":
# 	start = time.time()
# 	# result_queue = queue.Queue()

# 	tws = TwsApi()
# 	tws.run()
# 	tws.get_contract('MSFT')
# 	time.sleep(5)
# 	tws.stop()
# 	# tws.get_contracts(ticker_symbol="AAPL")
# 	# tws.order(reqId=3, ticker_symbol="AAPL", side="BUY", qty=5, type_order="MKT")
	
# 	# def find_detail_con(result_queue):
# 	# 	tws = TWSClient()
# 	# 	tws.find_detail(ticker_symbol="AAPL", args(result_queue,))

# 	# proc = multiprocessing.Process(target=find_detail_con)
# 	end = time.time()
# 	print(f"time consumed : {end-start}")
