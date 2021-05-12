from core.master.models import MasterOhlcvtr
from django.core.management.base import BaseCommand, CommandError

from core.orders.models import Order, PositionPerformance, OrderPosition
from core.Clients.models import UserClient, ClientTopStock
import pandas as pd
from core.djangomodule.serializers import CsvSerializer

import io


def export_csv(df):
  with io.StringIO() as buffer:
    df.to_csv(buffer,index=False)
    return buffer.getvalue()

class Command(BaseCommand):

    def handle(self, *args, **options):
        hanwha = [user["user"] for user in UserClient.objects.filter(
            client__client_name="HANWHA", extra_data__service_type="bot_advisor").values("user")]
        dates = [
            str(perf.created) for perf in PositionPerformance.objects.all().order_by("created").distinct("created")]
        curr = [
            "CNY",
            "HKD",
            "USD",
            "KRW",
        ]
        for created in dates:
            for currency in curr:
                perf = PositionPerformance.objects.filter(
                    position_uid__user_id__in=hanwha, created=created, position_uid__ticker__currency_code=currency).order_by("created")
                # item = json.dumps(CsvSerializer(perf, many=True).data)
                if perf.exists():
                    df = pd.DataFrame(CsvSerializer(perf, many=True).data)
                    df = df.fillna(0)
                    # df.to_csv(
                    #     f"files/file_csv/hanwha/{currency}/{currency}_{created}_asklora.csv", index=False)
                    files = export_csv(df)
                    print(files)
