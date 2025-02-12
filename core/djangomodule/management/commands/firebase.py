from django.core.management.base import BaseCommand
from ingestion.firestore_migration import firebase_user_update, firebase_universe_update
from general.firestore_query import get_price_data_firebase
import json
import pandas as pd
import threading
import time
from firebase_admin import firestore
import random
import requests
from core.universe.models import Universe
from django.conf import settings
def delete_collection(coll_ref, batch_size):
    docs = coll_ref.limit(batch_size).stream()
    deleted = 0

    for doc in docs:
        print(f'Deleting doc {doc.id} => {doc.to_dict()}')
        doc.reference.delete()
        deleted = deleted + 1

    if deleted >= batch_size:
        return delete_collection(coll_ref, batch_size)


def randomize_rank(data):
    import random
    r = random.randint(1,6)
    print(r,data)
    if r not in data:
        return r
        
    return randomize_rank(data)
class Command(BaseCommand):
    

    def handle(self, *args, **options):
        # data= get_price_data_firebase(['MSFT.O','AAPL.O'])
        # print(data)
        # user_id=643
        db = firestore.client()
        rank =db.collection(settings.FIREBASE_COLLECTION['ranking'])
        queryset = rank.get()
        while True:
            try:
                selected_rank =[]
                for data in queryset:
                    ref = rank.document(data.to_dict()['ranking'])
                    num = randomize_rank(selected_rank)
                    ref.set({'rank':num},merge=True)
                    selected_rank.append(num)
                    # print(selected_rank)
                selected_rank=[]
                print(selected_rank)

                time.sleep(20)
            except KeyboardInterrupt:
                break
        # collection =db.collection(settings.FIREBASE_COLLECTION['portfolio']).document(f"{user_id}")
        # while collection.get().exists:
        #     collection.delete()
        # user_data =db.collection(settings.FIREBASE_COLLECTION['portfolio']).where("id","==",f"{user_id}")
        # print(collection.get().exists)
        # for data in user_data.get():
        #     print(data.id)
        # firebase_user_update(user_id=[119])
        # firebase_universe_update(currency_code=["HKD","USD"])
        # db = firestore.client()
        # univ = Universe.objects.prefetch_related("currency_code").filter(currency_code="USD")
        # for ticker in univ:
        #     collection =db.collection(u"universe").document(f"{ticker.ticker}")
        #     collection.delete()
        # collection =db.collection(u"portfolio").where("user_id","==",638)
        # delete_collection(collection,1)
        # res = requests.delete("https://firestore.googleapis.com/v1/projects/asklora-android/databases/(default)/documents/portfolio/638")
        # print(res)
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