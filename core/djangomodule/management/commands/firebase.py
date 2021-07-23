from datetime import datetime
from django.core.management.base import BaseCommand
from ingestion.mongo_migration import mongo_universe_update
from core.djangomodule.serializers import HotUniverse, Universe
import json
import pandas as pd
import threading
import time
from firebase_admin import firestore
import random





class Command(BaseCommand):

    def handle(self, *args, **options):
        mongo_universe_update(currency_code=["HKD","CNY"])
        # db = firestore.client()
        # ticker = ['0998.HK','0267.HK','0883.HK']
        # while True:
        #     index = random.randint(0,2)
        #     ref = db.collection('universe').document(ticker[index])
        #     p = round(random.uniform(8,12),2)
        #     ref.set({'price':{'latest_price':p}},merge=True)
        #     time.sleep(1)
        # data = HotUniverse(Universe.objects.filter(currency_code__in=['HKD','CNY'],is_active=True),many=True).data
        # data = json.loads(json.dumps(data,indent=2))
        # df = pd.DataFrame(data)
        # df['indexes'] = df['ticker']
        # df = df.set_index('indexes')
        # df = df.to_dict('index')
        # db = firestore.client()
        # doc_ref = db.collection(u'universe')
        # doc_ref.on_snapshot(on_snapshot)


        # # Watch the document
        # while True:
        #     try:
        #         time.sleep(1)
        #         print('listening...')
        #     except Exception:
        #         break
        #     except KeyboardInterrupt:
        #         break

        # ref = db.collection('universe')
        # query = ref.where('detail.currency_code','==','CNY').where('ticker','not-in',['.CSI300''.HSI','.HSLI','.SPX'])
        # for data  in query.get():
        #     print(data.id)
        # for key,val in df.items():
        #     doc_ref = db.collection(u'universe').document(f'{key}')
        #     doc_ref.set(val)
        # ref =  db.reference(f'/universe')
        # ref.child('000001SZ/price').update({'close':25})
        # print(ref.child('000001SZ/price').get())
        # ref.set(df)
 
        # print(ref.get())
        # univ = db.reference('/universe')
        # print(univ.get())