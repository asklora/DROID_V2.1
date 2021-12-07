
from django.core.management.base import BaseCommand
from portfolio.factory.BaseFactory import BotBackendDirector
from core.bot.models import BotOptionType
class Command(BaseCommand):
    def handle(self, *args, **options):
        director = BotBackendDirector('UNO_OTM_016666')
        print( director.bot_processor)
       