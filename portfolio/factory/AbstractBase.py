from abc import ABC, abstractmethod








class AbstractCalculator(ABC):
    
    @abstractmethod
    def calculate(self):
        pass


class AbstractBotProcessor(ABC):
    
    
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
    def bot_use(self,name):
        pass


