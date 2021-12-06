from abc import ABC, abstractmethod








class AbstractCaltulator(ABC):
    
    @abstractmethod
    def calculate(self):
        pass


class Bot(ABC):
    
    
    @abstractmethod
    def create(self):
        pass
    
    @abstractmethod
    def hedge(self):
        pass
    
    @abstractmethod
    def stop(self):
        pass
    

    
    


class AbstactBotFactory(ABC):
    
    @abstractmethod
    def bot_select(self,name) -> Bot:
        pass


