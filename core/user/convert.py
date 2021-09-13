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

    
    def convert_money(self, amount, from_currency:str, to_currency:str):
        from_currency = Currency.objects.get(currency_code=from_currency)
        to_currency = Currency.objects.get(currency_code=to_currency)
        exchange_rate = from_currency.last_price * to_currency.last_price
        rounded = 0
        if(to_currency.is_decimal):
            rounded = 2
        if(from_currency.last_price <= to_currency.last_price):
            result = amount * exchange_rate
        else:
            result = amount / exchange_rate
        return round(result, rounded)
    
    def convert_money(self, amount, from_currency:Currency, to_currency:Currency):
        exchange_rate = from_currency.last_price * to_currency.last_price
        rounded = 0
        if(to_currency.is_decimal):
            rounded = 2
        if(from_currency.last_price <= to_currency.last_price):
            result = amount * exchange_rate
        else:
            result = amount / exchange_rate
        return round(result, rounded)