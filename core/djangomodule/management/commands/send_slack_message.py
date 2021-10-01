from django.core.management.base import BaseCommand
from general.slack import report_to_slack

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("-m", "--message", type=str, help="message")
    
    
    
    def handle(self, *args, **options):
        message = options.get('message',None)
        if message:
            message = "[PIPELINE UPDATE PRODUCTION] "+ message
            report_to_slack(message,channel='#droidv2-autodeploy')