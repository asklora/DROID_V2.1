
from typing_extensions import Protocol
from core.orders.models import Order
from typing import Union

class ValidatorProtocol(Protocol):
    
    def validate(self):
        ...



class GetPriceProtocol(Protocol):
    
    def get_price(self,ticker)->float:
        """
        get price should always have one arguments for ticker
        """
        ...
        
        
class OrderProtocol(Protocol):
    validator:ValidatorProtocol
    getter_price:GetPriceProtocol
    response:Union[Order,dict]
    
    def execute(self):
        ...