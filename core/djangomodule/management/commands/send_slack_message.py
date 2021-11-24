from django.core.management.base import BaseCommand
from general.slack import report_to_slack
from datetime import datetime
class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("-m", "--message", type=str, help="message")
    
    
    
    def handle(self, *args, **options):
        message = options.get('message',None)
        date = str(datetime.now())
        if message:
            message = "*[PIPELINE UPDATE PRODUCTION]* "+ message
            format_message=f"{date} :: ====== {message} ======"
            report_to_slack(format_message,channel='#droidv2-autodeploy')