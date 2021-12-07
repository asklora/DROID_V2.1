from .AbstractBase import *
from core.bot.models import BotOptionType
from .estimator import BlackScholes

class BaseProcessor(AbstractBotProcessor):

    def __init__(self,estimator:AbstractCalculator=BlackScholes):
        self.estimator=estimator()
        
        
        
        
class ClassicBot(BaseProcessor):
    pass



class UcdcBot(BaseProcessor):
    pass




class UnoBot(BaseProcessor):
    pass







class BotBackendDirector(AbstactBotFactory):
    bot_process = {
        "classic":ClassicBot,
        "ucdc":UcdcBot,
        "uno":UnoBot,
        
    }
    model : BotOptionType
    
    def __init__(self,bot_id:str):
        try:
            self.model = BotOptionType.objects.get(pk=bot_id)
        except BotOptionType.DoesNotExist:
            raise Exception('Bot does not exist')
        self.bot_use(self.model.bot_type.bot_type.lower())
    
    
    def bot_use(self, name):
        try:
            self.bot_processor = self.bot_process[name]
        except KeyError:
            raise Exception('Bot does not exist')
        
        
    
    





        
        
            
    
    