import math
from abc import abstractmethod, ABC
from core.universe.models import Currency
class AbstractBaseConvert(ABC):

    @abstractmethod
    def set_from_currency(self):
        pass

    @abstractmethod
    def set_to_currency(self):
        pass

    @abstractmethod
    def set_exchange_rate(self):
        pass

    @abstractmethod
    def get_from_currency(self):
        pass

    @abstractmethod
    def get_to_currency(self):
        pass

    @abstractmethod
    def get_exchange_rate(self):
        pass

    @abstractmethod
    def convert(self):
        pass

    @abstractmethod
    def to_hkd(self):
        pass

    @abstractmethod
    def to_usd(self):
        pass

    @abstractmethod
    def to_eur(self):
        pass

    @abstractmethod
    def to_cny(self):
        pass

    @abstractmethod
    def to_jpy(self):
        pass

    @abstractmethod
    def to_krw(self):
        pass

    @abstractmethod
    def to_gbp(self):
        pass

class ConvertMoney(AbstractBaseConvert):
    
    def __init__(self, from_cur:Currency, to_cur:Currency):
        self.set_from_currency(from_cur)
        self.set_to_currency(to_cur)
        
        self.set_exchange_rate()

    def set_from_currency(self, from_cur):
        if isinstance(from_cur,str):
            self.from_currency = Currency.objects.get(currency_code=from_cur.upper())
        elif isinstance(from_cur,Currency):
            self.from_currency = from_cur
        else:
            raise ValueError(f"currency wrong type {type(from_cur)}")

    def set_to_currency(self, to_cur):
        if isinstance(to_cur,str):
            self.to_currency = Currency.objects.get(currency_code=to_cur.upper())
        elif isinstance(to_cur,Currency):
            self.to_currency = to_cur
        else:
            raise ValueError(f"currency wrong type {type(to_cur)}")

    def set_exchange_rate(self):
        if(self.from_currency.last_price == self.to_currency.last_price):
            self.exchange_rate = 1
        else:
            self.exchange_rate = self.to_currency.last_price / self.from_currency.last_price

    def get_from_currency(self):
        return self.from_currency

    def get_to_currency(self):
        return self.to_currency

    def get_exchange_rate(self):
        return self.exchange_rate

    def convert(self, amount):
        if(self.to_currency.is_decimal):
            return math.floor(amount * self.exchange_rate * 100) / 100
        else:
            return math.floor(amount * self.exchange_rate)
    
    def to_hkd(self, amount):
        self.set_to_currency("HKD")
        self.set_exchange_rate()
        return self.convert(amount)

    def to_usd(self, amount):
        self.set_to_currency("USD")
        self.set_exchange_rate()
        return self.convert(amount)

    def to_eur(self, amount):
        self.set_to_currency("EUR")
        self.set_exchange_rate()
        return self.convert(amount)

    def to_cny(self, amount):
        self.set_to_currency("CNY")
        self.set_exchange_rate()
        return self.convert(amount)

    def to_jpy(self, amount):
        self.set_to_currency("JPY")
        self.set_exchange_rate()
        return self.convert(amount)

    def to_krw(self, amount):
        self.set_to_currency("KRW")
        self.set_exchange_rate()
        return self.convert(amount)

    def to_gbp(self, amount):
        self.set_to_currency("GBP")
        self.set_exchange_rate()
        return self.convert(amount)