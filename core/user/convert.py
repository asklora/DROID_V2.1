from abc import ABC,abstractmethod
from core.universe.models import Currency

class Convert():
    @abstractmethod
    def convert_money(self):
        pass

class ConvertMoney(Convert):
    

    def __init__(self,from_cur:str,to_cur:str):
        """
        init only define once and only called when the class invoked
        ex: ConvertMoney()


        define from cur and to cur with dependency injection from django models

        self.variable_name -> this can be called to entire class method,so we adjust one at the time in init
        now we have:
        - from_cur
        - to_cur
        - exchange_rate
        how it will called :
            usd_to_eur = ConvertMoney('USD','EUR') 
            usd_to_hkd = ConvertMoney('USD','HKD')
            # now we have converter object for use

            # and we can use this repeatedly without query again to the DB, except new converter
            usd_to_eur.convert(500)
            usd_to_hkd.convert(100)
        """
        # TODO: add exception handler
        self.from_currency = Currency.objects.get(currency_code=from_cur.upper())
        self.to_currency = Currency.objects.get(currency_code=to_cur.upper())
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