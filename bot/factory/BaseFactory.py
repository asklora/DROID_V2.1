import asyncio
from typing import List
from bot.factory.validator import BotCreateProps
from .AbstractBase import AbstactBotDirector, AbstractBotProcessor
from core.bot.models import BotOptionType
from .estimator import BlackScholes, UnoCreateEstimator, UcdcCreateEstimator
from .bot_protocols import ValidatorProtocol, EstimatorProtocol
from .botproperties import ClassicProperties
from .act.creator import ClassicCreator, Creator, UnoCreator, UcdcCreator
from asgiref.sync import sync_to_async
from .BotException import UnactiveTicker


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


class BatchCreateExecutor:
    result_dict_data = []
    result_dict_class = []

    def __init__(self, directors: List[BotCreateDirector]):
        self.directors = directors

    async def _initialize_class(self, director):
        return await sync_to_async(director.create)()

    async def main(self):
        tasks = []
        for director in self.directors:
            tasks.append(
                asyncio.ensure_future(self._initialize_class(director))
            )
        return await asyncio.gather(*tasks)

    def create(self):
        results = asyncio.run(self.main())

        for result in results:
            self.result_dict_data.append(result.get_result_as_dict())
            self.result_dict_class.append(result.get_result())
        return self

    def get_result_as_dict(self):
        return self.result_dict_data

    def get_result(self):
        return self.result_dict_class


class BotHedgeDirector(BaseBackendDirector):
    def __init__(self, props: ValidatorProtocol):
        try:
            props.validate()
        except UnactiveTicker:
            pass
        self.props = props
        self.bot_use(self.props.bot.bot_type.bot_type.lower(), props)

    def create(self):
        return self.bot_processor.hedge()


class BotStopDirector(BaseBackendDirector):
    pass


class BotFactory:

    """
    method available
        - get_creator(props:ValidatorProtocol) -> BotCreateDirector
        - get_hedger -> BotHedgeDirector
        - get_stopper -> BotStopDirector
    """

    async def _initialize_class(self, props):
        self._check_props_class(props)
        return await sync_to_async(BotCreateDirector)(props)

    async def main(self, props):
        tasks = []
        for p in props:
            tasks.append(asyncio.ensure_future(self._initialize_class(p)))
        return await asyncio.gather(*tasks)

    def _check_props_class(self, props):
        if not isinstance(props, BotCreateProps):
            raise TypeError(" creator Props must be BotCreateProps")

    def get_batch_creator(self, props):
        directors = asyncio.run(self.main(props))
        return BatchCreateExecutor(directors)

    def get_creator(self, props: ValidatorProtocol) -> BaseProcessor:
        self._check_props_class(props)
        director = BotCreateDirector(props)
        return director

    def get_hedger(self, props) -> BaseProcessor:
        director = BotHedgeDirector(props)
        return director.bot_processor

    def get_stopper(self, props) -> BaseProcessor:
        director = BotStopDirector(props)
        return director.bot_processor
