from .rkd import RkdData
from .yahooFin import get_quote_yahoo


class RkdGetterPrice:

    def get_price(self, ticker:list)->float:
        rkd = RkdData(timeout=3)
        df = rkd.get_quote(
            ticker, save=True, df=True)
        df["latest_price"] = df["latest_price"].astype(float)
        return df.iloc[0]["latest_price"]




class YahooGetterPrice:

    def get_price(self, ticker:str)->float:
        df= get_quote_yahoo(ticker)
        df["latest_price"] = df["latest_price"].astype(float)
        return df.iloc[0]["latest_price"]