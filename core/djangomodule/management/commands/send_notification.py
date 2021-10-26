from django.core.management.base import BaseCommand

from core.services.notification import send_notification, send_bulk_notification




class Command(BaseCommand):
    
    
    
    def add_arguments(self, parser):
        parser.add_argument("-u", "--username", type=str, help="username")
        parser.add_argument("-t", "--title", type=str, help="title")
        parser.add_argument("-m", "--message", type=str, help="message")
        parser.add_argument("-b", "--bulk", action="store_true", help="bulk message", default=False)
    
    
    
    def handle(self, *args, **options):
        if options.get("username",None) and options.get("title",None) and options.get("message",None):
            send_notification(
                options.get("username"),
                options.get("title"),
                options.get("message")
                )
        if options.get("bulk",False) and options.get("title",None) and options.get("message",None):
            send_bulk_notification(
                options.get("title"),
                options.get("message"),
            )
        