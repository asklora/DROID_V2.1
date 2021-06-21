from core.services.tasks import send_csv_hanwha

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("-currency", "--currency", type=str, help="currency")
    
    
    
    def handle(self, *args, **options):
        send_csv_hanwha(currency=options['currency'])