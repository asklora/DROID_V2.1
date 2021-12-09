from .AbstractBase import *
from core.bot.models import BotOptionType
from .estimator import BlackScholes
from .validator import BotCreateProps
from .bot_protocols import ValidatorProtocol,EstimatorProtocol
import math
from core.master.models import LatestPrice
from .botproperties import ClassicProperties


class BaseProcessor(AbstractBotProcessor):

    def __init__(self,validated_data:ValidatorProtocol,estimator:EstimatorProtocol=BlackScholes):
        self.validated_data = validated_data
        self.estimator= estimator()
        
        
        
        
class ClassicBot(BaseProcessor):
    # def get_classic(ticker, spot_date, time_to_exp, investment_amount, price, expiry_date,margin:int=1):
    #     spot_date = check_date(spot_date)
    #     expiry_date = check_date(expiry_date)
    #     digits = max(min(4-len(str(int(price))), 2), -1)
    #     classic_vol_data = get_classic_vol_by_date(ticker, spot_date)
    #     classic_vol = classic_vol_data["classic_vol"]

    #     month = int(round((time_to_exp * 365), 0)) / 30
    #     dur = pow(time_to_exp, 0.5) * min((0.75 + (month * 0.25)), 2)

    #     data = {
    #         "price": price,
    #         "performance":{},
    #         "position":{}
    #     }
    #     total_bot_share_num = math.floor((investment_amount * margin) / price)
    #     bot_cash_balance =round(investment_amount - (total_bot_share_num * price), 2)
    #     data["performance"]["vol"] = dur
    #     data["performance"]["last_hedge_delta"] = 1
    #     data["performance"]["share_num"] = total_bot_share_num
    #     data['performance']["current_bot_cash_balance"] = bot_cash_balance
    #     data["position"]["expiry"] = expiry_date.date().strftime("%Y-%m-%d")
    #     data["position"]["spot_date"] = spot_date.date().strftime("%Y-%m-%d")
    #     data["position"]["total_bot_share_num"] = total_bot_share_num
    #     data["position"]["max_loss_pct"] = - (dur * classic_vol * 1.25)
    #     data["position"]["max_loss_price"] = round(price * (1 + data["position"]["max_loss_pct"]), int(digits))
    #     data["position"]["max_loss_amount"] = round((data["position"]["max_loss_price"] - price) * total_bot_share_num, int(digits))
    #     data["position"]["target_profit_pct"] = (dur * classic_vol)
    #     data["position"]["target_profit_price"] = round(price * (1 + data["position"]["target_profit_pct"]), digits)
    #     data["position"]["target_profit_amount"] = round((data["position"]["target_profit_price"] - price) * total_bot_share_num, digits)
    #     data["position"]["bot_cash_balance"] = bot_cash_balance
    #     data["position"]["investment_amount"]=investment_amount
    #     return data
    
    def create(self):
        print(self.validated_data.expiry)
    
    
    def get_vol(self):
        try:
            return LatestPrice.objects.get(ticker=self.validated_data.ticker).classic_vol
        except LatestPrice.DoesNotExist:
            raise ValueError("Ticker not found in latest price")

        
    
    
    
    def hedge(self):
        pass
    
    def stop(self):
        pass



class UcdcBot(BaseProcessor):
    
    
    def create(self):
        print(self.validated_data.expiry)

    
    def hedge(self):
        pass
    
    def stop(self):
        pass




class UnoBot(BaseProcessor):
    
    
    
    def create(self):
        print(self.validated_data.expiry)

    
    def hedge(self):
        pass
    
    def stop(self):
        pass



class BaseBackendDirector(AbstactBotDirector):
    bot_process = {
        "classic":ClassicBot,
        "ucdc":UcdcBot,
        "uno":UnoBot,
        
    }
    
    model : BotOptionType
    

    
    def bot_use(self, name,props:ValidatorProtocol):
        try:
            self.bot_processor = self.bot_process[name](props)
        except KeyError:
            raise Exception('Bot does not exist')
        
    def set_estimator(self,estimator:EstimatorProtocol):
        self.bot_processor.estimator = estimator()
    
    
        
class BotCreator(BaseBackendDirector):
    
    
    def __init__(self,props:ValidatorProtocol):
        props.validate()
        self.props = props
        self.bot_use(self.props.bot.bot_type.bot_type.lower(),props)
        
    def create(self):
        self.bot_processor.create()
    
    
    

class BotHedger(BaseBackendDirector):
    pass

class BotStopper(BaseBackendDirector):
    pass


class BotFactory:
    
    """
    method available
        - get_creator(props:ValidatorProtocol) -> BotCreator
        - get_hedger -> BotHedger
        - get_stopper -> BotStopper
    """
    
    
    def get_creator(self,props:ValidatorProtocol) -> BotCreator:
        return BotCreator(props)
    
    def get_hedger(self,props) -> BotHedger:
        return BotHedger(props)
    
    def get_stopper(self,props) -> BotStopper:
        return BotStopper(props)
    
        
        
    
    





        
        
            
    
    