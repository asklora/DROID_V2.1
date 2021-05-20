from django.core.management.base import BaseCommand, CommandError
import pandas as pd
from datetime import datetime
from core.universe.models import Universe, ExchangeMarket
import json

class Command(BaseCommand):

    def handle(self, *args, **options):
        df = pd.read_json(open('files/file_json/tradinghours_all_market.json'))
        df= df.to_json(orient="records")
        df =  json.loads(df)
        for data in df:
            mic = Universe.objects.filter(mic=data['mic'])
            if mic.exists():
                data['currency_code_id'] = mic.first().currency_code.currency_code
            data['created']=datetime.now()
            data['updated']=datetime.now()
        model_instances = [ExchangeMarket(**field) for field in df]
        ExchangeMarket.objects.bulk_create(model_instances)
        print(df)
        
        
