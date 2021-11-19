from core.services.order_services import order_executor
from .payload import ActionPayload,SellPayload,BuyPayload
from datasource.getterprice import RkdGetterPrice
from django.utils import timezone
from core.orders.models import Order
from .order_protocol import ValidatorProtocol, OrderProtocol,GetPriceProtocol
from .validator import SellValidator,BuyValidator,ActionValidator
from rest_framework import exceptions
from django.db import transaction as db_transaction
from portfolio import (
    classic_sell_position,
    ucdc_sell_position,
    uno_sell_position,
    user_sell_position
)
import json


class SellOrderProcessor:
    getter_price = RkdGetterPrice()
    response: Order = None
    

    def __init__(self, payload: dict,getterprice:GetPriceProtocol=None):
        self.payload = SellPayload(**payload)
        self.validator: ValidatorProtocol = SellValidator(self.payload)
        if getterprice:
            self.getter_price = getterprice

    def execute(self):
        self.payload.price = self.getter_price.get_price([self.payload.ticker.ticker])
        with db_transaction.atomic():
            position = self.validator.position
            bot = position.bot
            trading_day=timezone.now()
            if bot.is_ucdc():
                positions, self.response= ucdc_sell_position(self.payload.price, trading_day, position,apps=True)
            elif bot.is_uno():
                positions, self.response=uno_sell_position(self.payload.price, trading_day, position,apps=True)
            elif bot.is_classic():
                positions, self.response=classic_sell_position(self.payload.price, trading_day, position,apps=True)
            elif bot.is_stock():
                positions, self.response=user_sell_position(self.payload.price, trading_day, position, apps=True)


class BuyOrderProcessor:
    getter_price = RkdGetterPrice()
    response: Order = None

    def __init__(self, payload: dict, getterprice:GetPriceProtocol=None):
        self.raw_payload=payload
        self.payload = BuyPayload(**payload)
        self.validator: ValidatorProtocol = BuyValidator(self.payload)
        if getterprice:
            self.getter_price = getterprice
     

    def execute(self):
        self.raw_payload["price"] = self.getter_price.get_price([self.payload.ticker.ticker])
        with db_transaction.atomic():
            self.response = Order.objects.create(
                **self.raw_payload, order_type='apps', is_init=True)



class BuyActionProcessor:
    
    getter_price = RkdGetterPrice()
    response: dict

    def __init__(self, payload: dict, getterprice:GetPriceProtocol=None):
        self.raw_payload=payload
        self.payload = ActionPayload(**payload)
        self.validator: ValidatorProtocol = ActionValidator(self.payload)
        if getterprice:
            self.getter_price = getterprice
            
            
class CancelActionProcessor:
    getter_price = RkdGetterPrice()
    response: dict

    def __init__(self, payload: dict, getterprice:GetPriceProtocol=None):
        self.raw_payload=payload
        self.payload = ActionPayload(**payload)
        self.validator: ValidatorProtocol = ActionValidator(self.payload)
        if getterprice:
            self.getter_price = getterprice

class SellActionProcessor:
    
    getter_price = RkdGetterPrice()
    response: dict

    def __init__(self, payload: dict, getterprice:GetPriceProtocol=None):
        self.raw_payload=payload
        self.payload = ActionPayload(**payload)
        self.validator: ValidatorProtocol = ActionValidator(self.payload)
        if getterprice:
            self.getter_price = getterprice



class ActionProcessor:
    getter_price = RkdGetterPrice()
    response: dict

    def __init__(self, payload: dict, getterprice:GetPriceProtocol=None):
        self.raw_payload=payload
        self.payload = ActionPayload(**payload)
        self.validator: ValidatorProtocol = ActionValidator(self.payload)
        if getterprice:
            self.getter_price = getterprice
     

    def execute(self):
        task_payload:str = json.dumps(self.raw_payload)
        task=order_executor.apply_async(args=(task_payload,),task_id=self.payload.order_uid)
        self.response={"action_id": task.id, "status": "executed",
                "order_uid": self.payload.order_uid}

class ActionOrderController:
    PROCESSOR={
        "sell":SellActionProcessor,
        "buy":BuyActionProcessor,
        "cancel":CancelActionProcessor
    }
    protocol:OrderProtocol

    def select_process_class(self,classname):
        self.protocol = self.PROCESSOR[classname]
    
    def process(self):
        self.processor.validator.validate()
        try:
            self.processor.execute()
        except Exception as e:
            raise exceptions.APIException({"detail": str(e)})
        return self.processor.response

class OrderController:

    def process(self, processor: OrderProtocol):
        processor.validator.validate()
        try:
            processor.execute()
        except Exception as e:
            raise exceptions.APIException({"detail": str(e)})
        return processor.response
    
 


OrderProcessor:dict={
    "buy":BuyOrderProcessor,
    "sell":SellOrderProcessor,
    "action":ActionProcessor
    }