from .AbstractBase import *
from core.bot.models import BotOptionType


class BlackScholes(AbstractCaltulator):
    pass






class ClassicBot(Bot):
    
    
    
    def __name__(self):
        return 'Classic'



class UcdcBot(Bot):
    
    
    
    def __name__(self):
        return 'Ucdc'




class UnoBot(Bot):
    
    
    def __name__(self):
        return 'Uno'







class BotBackendBase(Bot):
    bot : BotOptionType
    
    def __init__(self,bot_id):
        try:
            self.bot = BotOptionType.objects.get(pk=bot_id)
        except BotOptionType.DoesNotExist:
            raise Exception('Bot does not exist')


class BotDirector(AbstactBotFactory):
    bot_process = {
        ClassicBot.__name__:ClassicBot,
        UcdcBot.__name__:UcdcBot,
        UnoBot.__name__:UnoBot,
        
    }
    
    
    def bot_select(self, name):
        try:
            return self.bot_process[name]
        except KeyError:
            raise Exception('Bot does not exist')
        
        
    
    





        
        
            
    
    