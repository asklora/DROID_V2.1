import requests
import json
import time
import websocket
import ssl
import queue

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.order import (OrderComboLeg, Order)
from ibapi.contract import Contract

from threading import Thread
from multiprocessing import Process
from gateway import *



class TestApp(EWrapper, EClient):
	def __init__(self):		
		EClient.__init__(self, self)

	def connect(self, host, port, clientId, queue):
		super().connect(host, port, clientId)
		self.result_queue = queue

	def error(self, reqId, errorCode, errorString):
		print("Error: ", reqId, " ", errorCode, " ", errorString)

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
		self.disconnect()

	def symbolSamples(self, reqId, contractDescriptions):
		super().symbolSamples(reqId, contractDescriptions)
		print("Symbol Samples. Request Id: ", reqId)
		messages = []
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
			messages.append(contract)			
			self.result_queue.put(messages)
		self.disconnect()

	def openOrder(self, orderId, contract, order, orderState):
		super().openOrder(orderId, contract, order, orderState)
		print(f"orderId : {orderId}")
		print(f"contract : {contract}")
		print(f"order : {order}")
		print(f"orderState : {orderState}")

	def orderStatus(self, orderId , status, filled,remaining, avgFillPrice, permId,parentId, lastFillPrice, clientId,whyHeld, mktCapPrice):
		super().orderStatus(orderId , status, filled,remaining, avgFillPrice, permId,parentId, lastFillPrice, clientId,whyHeld, mktCapPrice)
		print(f"orderId{orderId}")
		print(f"status{status}")
		print(f"filled{filled}")
		print(f"remaining{remaining}")
		print(f"avgFillPrice{avgFillPrice}")
		print(f"permId{permId,parentId}")
		print(f"lastFillPrice{lastFillPrice}")
		print(f"clientId{clientId}")
		print(f"whyHeld{whyHeld}")
		print(f"mktCapPrice{mktCapPrice}")


class TWSClient():

	def __init__(self):
		self.host = "0.tcp.ngrok.io"
		self.port = 10613
		self.result_queue = queue.Queue()
		# self.result_queue = result_queue

	def get_contracts(self, ticker_symbol):
		app = TestApp()
		app.connect(self.host, self.port, 0, self.result_queue)
		print("requesting symbols ...")
		app.reqMatchingSymbols(2, ticker_symbol)
		print("running WS")
		app.run()

		res = None
		# contract = None
		contract = Contract()
		res_detail = None
	
		while True:
			try:
				res = self.result_queue.get(timeout=0.5)
				# app.disconnect()
				break
			except queue.Empty:
				print("Queue Empty")

		for data in res:
			if data['symbol'] == ticker_symbol:
				if data['secType'] == 'STK':
					if data['currency'] == 'USD':
						contract.symbol = ticker_symbol
						contract.secType = data['secType']
						contract.exchange = "SMART"
						contract.currency = data['currency']
						contract.primaryExchange = data['primaryExchange']
						res_detail = data

		print(f"response = {res_detail}")
		return contract

	def find_contract_detail(self, ticker_symbol):
		contract = self.get_contracts(ticker_symbol)
		app = TestApp()
		app.connect(self.host, self.port, 0, self.result_queue)
		print("requesting contract ...")
		app.reqContractDetails(3, contract)
		print("running WS")
		app.run()

	def order(self, ticker_symbol, side, qty, type_order, price=None):
		contract = self.get_contracts(ticker_symbol)

		app = TestApp()
		app.connect(self.host, self.port, 0, self.result_queue)
		order = Order()
		order.action = side
		order.totalQuantity = qty
		order.orderType = type_order

		if type_order == "LMT":
			order.lmtPrice = price

		app.placeOrder(5, contract, order)
		app.run()


if __name__=="__main__":
	start = time.time()
	result_queue = queue.Queue()

	tws = TWSClient()
	# tws.get_contracts(ticker_symbol="AAPL")
	tws.order(ticker_symbol="AAPL", side="BUY", qty=5, type_order="MKT")
	# def find_detail_con(result_queue):
	# 	tws = TWSClient()
	# 	tws.find_detail(ticker_symbol="AAPL", args(result_queue,))

	# proc = multiprocessing.Process(target=find_detail_con)
	end = time.time()
	print(f"time consumed : {end-start}")
