from .AbstractBase import AbstactBotDirector, AbstractBotProcessor
from core.bot.models import BotOptionType
from .estimator import BlackScholes, UnoCreateEstimator, UcdcCreateEstimator
from .bot_protocols import ValidatorProtocol, EstimatorProtocol
from .botproperties import ClassicProperties
from .act.creator import ClassicCreator, Creator, UnoCreator, UcdcCreator


class BaseProcessor(AbstractBotProcessor):
    def __init__(
        self,
        validated_data: ValidatorProtocol,
        estimator: EstimatorProtocol = BlackScholes,
    ):
        self.validated_data = validated_data
        self.estimator = estimator()

    def set_estimator(self, estimator: EstimatorProtocol):
        self.estimator = estimator()


class ClassicBot(BaseProcessor):

    properties: ClassicProperties

    def create(self) -> Creator:
        creator = ClassicCreator(self.validated_data, self.estimator)
        creator.process()
        return creator

    def hedge(self):
        pass

    def stop(self):
        pass


class UcdcBot(BaseProcessor):
    def create(self):
        super().set_estimator(UcdcCreateEstimator)
        creator = UcdcCreator(self.validated_data, self.estimator)
        creator.process()
        return creator

    def hedge(self):
        pass

    def stop(self):
        pass


class UnoBot(BaseProcessor):
    def create(self) -> Creator:
        super().set_estimator(UnoCreateEstimator)
        creator = UnoCreator(self.validated_data, self.estimator)
        creator.process()
        return creator

    def hedge(self):
        pass

    def stop(self):
        pass


class BaseBackendDirector(AbstactBotDirector):
    bot_process = {
        "classic": ClassicBot,
        "ucdc": UcdcBot,
        "uno": UnoBot,
    }

    model: BotOptionType

    def bot_use(self, name, props: ValidatorProtocol):
        try:
            self.bot_processor = self.bot_process[name](props)
        except KeyError:
            raise Exception("Bot does not exist")
        
    def create(self):
        return self.bot_processor.create()


class BotCreateDirector(BaseBackendDirector):
    def __init__(self, props: ValidatorProtocol):
        props.validate()
        self.props = props
        self.bot_use(self.props.bot.bot_type.bot_type.lower(), props)


class BotHedgeDirector(BaseBackendDirector):
    pass


class BotStopDirector(BaseBackendDirector):
    pass


class BotFactory:

    """
    method available
        - get_creator(props:ValidatorProtocol) -> BotCreateDirector
        - get_hedger -> BotHedgeDirector
        - get_stopper -> BotStopDirector
    """

    def get_creator(self, props: ValidatorProtocol) -> BaseProcessor:
        director = BotCreateDirector(props)
        return director

    def get_hedger(self, props) -> BaseProcessor:
        director = BotHedgeDirector(props)
        return director.bot_processor

    def get_stopper(self, props) -> BaseProcessor:
        director = BotStopDirector(props)
        return director.bot_processor
