from core.services.notification import send_notification as firebase_send_notification
from django.utils import timezone
import logging
from core.orders.models import Order
from .order_protocol import ValidatorProtocol, OrderProtocol, GetPriceProtocol
from .payload import ActionPayload, SellPayload, BuyPayload
from .validator import (
    BuyValidator,
    CancelExecutorValidator,
    ExecutorValidator,
    SellValidator,
    ActionValidator,
)
from rest_framework import exceptions
from django.db import transaction as db_transaction
from datetime import datetime
from portfolio import (
    classic_sell_position,
    ucdc_sell_position,
    uno_sell_position,
    user_sell_position,
)
import asyncio
import json
from datasource.getterprice import RkdGetterPrice
from channels.layers import get_channel_layer
from config.celery import app
from django.apps import apps


@app.task(bind=True)
def order_executor(self, payload:str, recall=False):
    if isinstance(payload,str):
        payload = json.loads(payload)

    controller = ActionOrderController()
    controller.select_process_class(payload)
    controller.process()

class SellOrderProcessor:
    getter_price = RkdGetterPrice()

    def __init__(self, payload: dict, getterprice: GetPriceProtocol = None):
        self.payload = SellPayload(**payload)
        self.validator: ValidatorProtocol = SellValidator(self.payload)
        if getterprice:
            self.getter_price = getterprice

    def execute(self):
        self.payload.price = self.getter_price.get_price([self.payload.ticker.ticker])
        with db_transaction.atomic():
            position = self.validator.position
            bot = position.bot
            trading_day = timezone.now()
            if bot.is_ucdc():
                positions, self.response = ucdc_sell_position(
                    self.payload.price, trading_day, position, apps=True
                )
            elif bot.is_uno():
                positions, self.response = uno_sell_position(
                    self.payload.price, trading_day, position, apps=True
                )
            elif bot.is_classic():
                positions, self.response = classic_sell_position(
                    self.payload.price, trading_day, position, apps=True
                )
            elif bot.is_stock():
                positions, self.response = user_sell_position(
                    self.payload.price, trading_day, position, apps=True
                )


class BuyOrderProcessor:
    getter_price = RkdGetterPrice()
    response: Order = None

    def __init__(self, payload: dict, getterprice: GetPriceProtocol = None):
        self.raw_payload = payload
        self.payload = BuyPayload(**payload)
        self.validator: ValidatorProtocol = BuyValidator(self.payload)
        if getterprice:
            self.getter_price = getterprice

    def execute(self):
        self.raw_payload["price"] = self.getter_price.get_price(
            [self.payload.ticker.ticker]
        )
        with db_transaction.atomic():
            self.response = Order.objects.create(
                **self.raw_payload, order_type="apps", is_init=True
            )
    


class BaseAction:
    channel_layer = get_channel_layer()
    getter_price = RkdGetterPrice()
    response: dict
    validator: ValidatorProtocol

    def execute(self):
        self.update_order()
        self.exchange_executor()
        self.send_response()

    def exchange_executor(self):
        ExchangeModel = apps.get_model("universe", "ExchangeMarket")
        try:
            exchange = ExchangeModel.objects.get(mic=self.validator.order.ticker.mic)
        except ExchangeModel.DoesNotExist as e:
            logging.error(e)
            self.message_error(f"{self.validator.order.ticker.mic} is not supported")
            self.send_response()
            raise ValueError(f"{self.validator.order.ticker.mic} is not supported")

        print(f"exchange is: {'open' if exchange.is_open else 'closed'}")

        if exchange.is_open:
            return self.fill_order()
        else:
            return self.message_pending()

    def fill_order(self):
        self.validator.order.status = "filled"
        self.validator.order.filled_at = datetime.now()
        with db_transaction.atomic():
            try:
                self.validator.order.save()
                self.validator.order.populate_to_firebase()
                return self.message_filled()
            except Exception as e:
                logging.error(str(e))
                self.message_error(str(e))
                raise ValueError(f"{self.validator.order.ticker} is not executed")

    def message_error(self, error: str):
        self.response = {
            "type": "send_order_message",
            "message_type": "order_error",
            "message": error,
            "status_code": 400,
        }

    def message_pending(self):
        self.response = {
            "type": "send_order_message",
            "message_type": "order_pending",
            "title": "order pending",
            "message": f"{self.validator.order.side} order {self.validator.order.qty} \
                            stocks {self.validator.order.ticker.ticker} is received, status pending",
            # 'payload': payload_serializer,
            "status_code": 200,
        }

    def message_cancel(self):
        self.response = {
            "type": "send_order_message",
            "message_type": "order_cancel",
            "title": "order cancel",
            "message": f"{self.validator.order.side} order {self.validator.order.qty} \
                            stocks {self.validator.order.ticker.ticker} is received, status canceled",
            # 'payload': payload_serializer,
            "status_code": 200,
        }

    def message_filled(self):
        self.response = {
                        'type': 'send_order_message',
                        'message_type': 'order_filled',
                        'title': 'order filled',
                        'message':f'{self.validator.order.side} order {self.validator.order.qty} \
                            stocks {self.validator.order.ticker.ticker} was executed, status filled',
                        
                        # 'payload': payload_serializer,
                        'status_code': 200
                }
        
    def order_in_pending(self):
        return self.validator.order.status == 'pending'
        
    def send_notification(self):
        return firebase_send_notification(self.validator.order.user_id.username, 
                                 self.response.get('title'),
                                 self.response.get('message')
                                 )        
    
    
    def send_response(self):
        return asyncio.run(
            self.channel_layer.group_send(str(self.validator.order.pk), self.response)
        )

    def update_order(self):
        self.validator.order.status = self.payload.status
        self.validator.order.placed = True
        self.validator.order.placed_at = datetime.now()
        self.validator.order.placed = True
        with db_transaction.atomic():
            try:
                self.validator.order.save()
            except Exception as e:
                logging.error(str(e))
                self.message_error(f"{self.validator.order.pk} update failed")
                self.send_response()
                raise ValueError(f'{self.validator.order.pk} update failed')

class BuyActionProcessor(BaseAction):
    def __init__(self, payload: dict, getterprice: GetPriceProtocol = None):
        self.raw_payload = payload
        self.payload = ActionPayload(**payload)
        self.validator: ValidatorProtocol = ExecutorValidator(self.payload)
        if getterprice:
            self.getter_price = getterprice

    def execute(self):
        if self.order_in_pending():
            self.recalculate_buy_order()
        super().execute()
    
    
    
    def refund_pending(self):
        """
        [summary]
            function will trigered buy recall
            
        """ 
        TransactionHistory = apps.get_model('user', 'TransactionHistory')
        in_wallet_transactions = TransactionHistory.objects.filter(
            transaction_detail__order_uid=str(self.validator.order.pk))
        if in_wallet_transactions.exists():
            with db_transaction.atomic():
                try:
                    in_wallet_transactions.delete()
                except Exception as e:
                    logging.error(str(e))
                    err_msg=f'{self.validator.order.pk} refund pending buy order failed'
                    self.message_error(err_msg)
                    self.send_response()
                    raise ValueError(err_msg)
                
        return self.reset_buy_order()
    
    def reset_buy_order(self):
        if self.validator.order.is_bot_order:
            self.validator.order.amount =self.validator.order.userconverter.convert(self.validator.order.setup["position"]["investment_amount"])
        else:
            if (self.validator.order.amount / self.validator.order.margin) > 11000:
                self.validator.order.amount = 20000
            else:
                self.validator.order.amount = 10000
        self.validator.order.price = self.getter_price.get_price([self.validator.order.ticker])
        self.validator.order.status = 'review'
        self.validator.order.placed = False
        self.validator.order.placed_at = None
        self.validator.order.qty = None
        with db_transaction.atomic():
            try:
                self.validator.order.save()
            except Exception as e:
                logging.error(str(e))
                self.message_error(f'{self.validator.order.pk} recalculate order failed')
                self.send_response()
                raise ValueError(f'{self.validator.order.pk} recalculate order failed')
            
    def recalculate_buy_order(self):
        return self.refund_pending()
        

class CancelActionProcessor(BaseAction):
    def __init__(self, payload: dict, getterprice: GetPriceProtocol = None):
        self.raw_payload = payload
        self.payload = ActionPayload(**payload)
        self.validator: ValidatorProtocol = CancelExecutorValidator(self.payload)
        if getterprice:
            self.getter_price = getterprice

    def execute(self):
        self.update_order()
        self.message_cancel()
        self.send_response()
        self.send_notification()

    def update_order(self):
        self.validator.order.status = self.payload.status
        self.validator.order.canceled_at = datetime.now()
        with db_transaction.atomic():
            try:
                self.validator.order.save()
            except Exception as e:
                logging.error(str(e))
                self.message_error(f"{self.validator.order.pk} update failed")
                self.send_response()
                raise ValueError(f"{self.validator.order.pk} update failed")


class SellActionProcessor(BaseAction):
    def __init__(self, payload: dict, getterprice: GetPriceProtocol = None):
        self.raw_payload = payload
        self.payload = ActionPayload(**payload)
        self.validator: ValidatorProtocol = ExecutorValidator(self.payload)
        if getterprice:
            self.getter_price = getterprice

    def execute(self):
        """
        #TODO: this need recalculate, but sell functionality need to revamp
        because of this function only create new order for now
        classic_sell_position
        ucdc_sell_position
        uno_sell_position
        user_sell_position
        """
        if self.order_in_pending():
            self.exchange_executor()
            self.send_response()
        else:
            super().execute()


class ActionProcessor:
    getter_price = RkdGetterPrice()
    response: dict

    def __init__(self, payload: dict, getterprice: GetPriceProtocol = None):
        self.raw_payload = payload
        self.payload = ActionPayload(**payload)
        self.validator: ValidatorProtocol = ActionValidator(self.payload)
        if self.raw_payload["status"] == "cancel":
            self.raw_payload["side"] = "cancel"
        else:
            self.raw_payload["side"] = self.validator.order.side
        if getterprice:
            self.getter_price = getterprice

    def execute(self):

        task_payload: str = json.dumps(self.raw_payload)
        task = order_executor.apply_async(
            args=(task_payload,), task_id=self.payload.order_uid
        )
        self.response = {
            "action_id": task.id,
            "status": "executed",
            "order_uid": self.payload.order_uid,
        }


class ActionOrderController:
    PROCESSOR = {
        "sell": SellActionProcessor,
        "buy": BuyActionProcessor,
        "cancel": CancelActionProcessor,
    }
    protocol: OrderProtocol

    def select_process_class(self, payload: dict):
        protocol = self.PROCESSOR[payload.pop("side")]
        self.protocol = protocol(payload)

    def process(self):
        try:
            self.protocol.validator.validate()
        except Exception as e:
            self.protocol.message_error(str(e))
            self.protocol.send_response()
            return
        try:
            self.protocol.execute()
        except Exception as e:
            logging.error(str(e))
            raise Exception({"detail": str(e)})
        return self.protocol.response


class OrderController:
    def process(self, protocol: OrderProtocol):
        protocol.validator.validate()
        try:
            protocol.execute()
        except Exception as e:
            raise exceptions.APIException({"detail": str(e)})
        return protocol.response


OrderProcessor: dict = {
    "buy": BuyOrderProcessor,
    "sell": SellOrderProcessor,
    "action": ActionProcessor,
}
