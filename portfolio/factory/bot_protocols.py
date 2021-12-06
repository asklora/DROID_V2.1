

from typing import Protocol




class HedgeProtocol(Protocol):
    
    
    
    def get_or_create_bot(self):
        ...
    
    
    def select_hedger_class(self):
        ...
    
    