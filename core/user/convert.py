from abc import ABC,abstractmethod
from core.universe.models import Currency

class AbstractBaseConvert(ABC):
    @abstractmethod
    def convert_money(self):
        pass

class Convert(AbstractBaseConvert):
    
    def __init__(self, from_cur, to_cur):
        if(type(from_cur) == str):
            self.from_currency = Currency.objects.get(currency_code=from_cur.upper())
        elif(type(from_cur) == Currency):
            self.from_currency = from_cur
        else:
            self.from_currency = Currency.objects.get(currency_code="USD")

        if(type(to_cur) == str):
            self.to_currency = Currency.objects.get(currency_code=to_cur.upper())
        elif(type(to_cur) == Currency):
            self.to_currency = to_cur
        else:
            self.to_currency = Currency.objects.get(currency_code="USD")
        self.exchange_rate = self.from_currency.last_price * self.to_currency.last_price


    def convert_money(self, amount):
        rounded = 0
        if(self.to_currency.is_decimal):
            rounded = 2
        if(self.from_currency.last_price <= self.to_currency.last_price):
            result = amount * self.exchange_rate
        else:
            result = amount / self.exchange_rate
        return round(result, rounded)
    
    # def convert_money(self, amount, from_currency:str, to_currency:str):
    #     from_currency = Currency.objects.get(currency_code=from_currency)
    #     to_currency = Currency.objects.get(currency_code=to_currency)
    #     exchange_rate = from_currency.last_price * to_currency.last_price
    #     rounded = 0
    #     if(to_currency.is_decimal):
    #         rounded = 2
    #     if(from_currency.last_price <= to_currency.last_price):
    #         result = amount * exchange_rate
    #     else:
    #         result = amount / exchange_rate
    #     return round(result, rounded)
    
    # def convert_money(self, amount, from_currency:Currency, to_currency:Currency):
    #     exchange_rate = from_currency.last_price * to_currency.last_price
    #     rounded = 0
    #     if(to_currency.is_decimal):
    #         rounded = 2
    #     if(from_currency.last_price <= to_currency.last_price):
    #         result = amount * exchange_rate
    #     else:
    #         result = amount / exchange_rate
    #     return round(result, rounded)