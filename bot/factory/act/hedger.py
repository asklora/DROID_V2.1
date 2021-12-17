from abc import ABC


class Hedger(ABC):

    pass


class BaseHedger(Hedger):
    pass


class ClassicHedger(BaseHedger):
    pass


class UnoHedger(BaseHedger):
    pass


class UcdcHedger(BaseHedger):
    pass
