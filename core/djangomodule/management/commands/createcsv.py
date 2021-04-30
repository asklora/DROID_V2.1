from django.core.management.base import BaseCommand, CommandError

from core.orders.models import Order, PositionPerformance, OrderPosition
from core.Clients.models import UserClient, ClientTopStock
import pandas as pd
from rest_framework import serializers
import json
from datetime import datetime


class CsvSerializer(serializers.ModelSerializer):
    bot_id = serializers.CharField(source='position_uid.bot_id')
    bot_status = serializers.SerializerMethodField()
    side = serializers.SerializerMethodField()
    position_id = serializers.CharField(source='position_uid.position_uid')
    ticker = serializers.CharField(source='position_uid.ticker.ticker')
    uuid = serializers.CharField(source='performance_uid')
    bot_share_num = serializers.IntegerField(source='share_num')
    delta = serializers.FloatField(source='last_hedge_delta')
    prev_delta = serializers.SerializerMethodField()
    prev_bot_share_num = serializers.SerializerMethodField()
    hedge_shares = serializers.SerializerMethodField()
    max_loss_price = serializers.FloatField(
        source='position_uid.max_loss_price')
    max_loss_pct = serializers.FloatField(source='position_uid.max_loss_pct')
    target_profit_price = serializers.FloatField(
        source='position_uid.target_profit_price')
    target_profit_pct = serializers.FloatField(
        source='position_uid.target_profit_pct')
    expired_in = serializers.CharField(
        source='position_uid.expiry')
    price = serializers.FloatField(
        source='last_live_price')
    entry_price = serializers.FloatField(
        source='last_spot_price')
    option_type = serializers.CharField(source='position_uid.bot.bot_type')
    service_type = serializers.SerializerMethodField()
    capital = serializers.SerializerMethodField()
    currency = serializers.CharField(
        source='position_uid.ticker.currency_code')

    class Meta:
        model = PositionPerformance
        # fields = '__all__'
        fields = ('service_type', 'capital', 'bot_id', 'option_type', 'created',
                  'side', 'bot_status', 'ticker', 'currency',  'current_investment_amount',
                  'entry_price', 'price', 'max_loss_price', 'max_loss_pct',
                  'target_profit_price',
                  'target_profit_pct',
                  'prev_bot_share_num', 'bot_share_num',  'hedge_shares', 'prev_delta', 'delta',
                  'strike', 'v1', 'v2',
                  'barrier', 'expired_in',
                  'uuid', 'position_id',

                  )

    def get_service_type(self, obj):
        top_stock = ClientTopStock.objects.filter(
            position_uid=obj.position_uid.position_uid)
        if top_stock.exists():
            top_stock = top_stock.get()
            return top_stock.service_type
        return '-'

    def get_capital(self, obj):
        top_stock = ClientTopStock.objects.filter(
            position_uid=obj.position_uid.position_uid)
        if top_stock.exists():
            top_stock = top_stock.get()
            return top_stock.capital
        return '-'

    def get_hedge_shares(self, obj):
        prev = PositionPerformance.objects.filter(
            position_uid=obj.position_uid, created__lt=obj.created).first()
        if prev:
            return int(obj.share_num - prev.share_num)
            # if prev.share_num > obj.share_num:
            #     return int(obj.share_num - prev.share_num)
            # elif prev.share_num < obj.share_num:
            #     return int(prev.share_num - obj.share_num)
            # else:
            #     return 0
        return obj.share_num

    def get_prev_delta(self, obj):
        prev = PositionPerformance.objects.filter(
            position_uid=obj.position_uid, created__lt=obj.created).first()
        if prev:
            return prev.last_hedge_delta
        return 0

    def get_prev_bot_share_num(self, obj):
        prev = PositionPerformance.objects.filter(
            position_uid=obj.position_uid, created__lt=obj.created).first()
        if prev:
            return int(prev.share_num)
        return 0

    def get_side(self, obj):
        if obj.order_uid:
            return obj.order_uid.side
        return 'hold'

    def get_bot_status(self, obj):
        if obj.order_uid:
            if obj.order_uid.is_init:
                return 'new'
            elif not obj.position_uid.is_live:
                return 'expired'
            else:
                return 'live'
        elif not obj.order_uid and not obj.position_uid.is_live:
            return 'expired'
        else:
            return 'live'


class Command(BaseCommand):

    def handle(self, *args, **options):
        hanwha = [user['user'] for user in UserClient.objects.filter(
            client__client_name='HANWHA', extra_data__service_type='bot_advisor').values('user')]
        dates = [
            '2021-04-26',
            '2021-04-27',
            '2021-04-28',
            '2021-04-29',
        ]
        for created in dates:
            perf = PositionPerformance.objects.filter(
                position_uid__user_id__in=hanwha, created=created).order_by('created')
            # item = json.dumps(CsvSerializer(perf, many=True).data)
            df = pd.DataFrame(CsvSerializer(perf, many=True).data)
            df = df.fillna(0)
            df.to_csv(f'{created}_asklora.csv', index=False)
