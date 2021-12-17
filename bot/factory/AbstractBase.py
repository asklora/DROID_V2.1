from abc import ABC, abstractmethod

from bot.factory.act.creator import Creator


class AbstractCalculator(ABC):
    @abstractmethod
    def calculate(self):
        pass


class AbstractBotProcessor(ABC):
    @abstractmethod
    def create(self) -> Creator:
        pass

    @abstractmethod
    def hedge(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def set_estimator(self, name, props):
        pass


class AbstractBotStopper(ABC):

    @abstractmethod
    def stop(self):
        pass


class AbstactBotDirector(ABC):
    @abstractmethod
    def bot_use(self, name, props):
        pass
