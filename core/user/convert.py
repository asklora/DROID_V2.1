from abc import ABC,abstractmethod
from core.universe.models import Currency

class Convert():
    @abstractmethod
    def convert_money(self):
        pass

class ConvertMoney(Convert):
    
    def __init__(self,*args,**kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)
    
    def run(self):
        """
        in here we delete the long if else statement
        will trigger the function inside this class with prefix name and invokes
        """
        func_name =f'on_{self.instance.side}_{self.instance.status}'
        
        if hasattr(self,func_name):
            """
            only invoke this function.
            hasattr will check the function name is exist or not 
           - on_buy_placed
            """
            function = getattr(self, func_name)
            function()

    
    def convert_money(self, amount, from_currency, to_currency):
        currency1 = Currency.objects.get(currency_code=from_currency)
        currency2 = Currency.objects.get(currency_code=to_currency)
        exchange_rate = currency1.last_price * currency2.last_price
        rounded = 0
        if(currency2.is_decimal):
            rounded = 2
        if(currency1.last_price <= currency2.last_price):
            result = amount * exchange_rate
        else:
            result = amount / exchange_rate
        return round(result, rounded)