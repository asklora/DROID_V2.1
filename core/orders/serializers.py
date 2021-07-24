from rest_framework import serializers
from .models import OrderPosition, PositionPerformance,OrderFee
from drf_spectacular.utils import OpenApiExample, extend_schema_serializer
from django.db.models import Sum,F
from core.user.models import TransactionHistory


@extend_schema_serializer(
    examples = [
         OpenApiExample(
            'Get bot Performance by positions',
            description='Get bot Performance by positions',
            value=[{
                    "created": "2021-05-27T05:25:55.776Z",
                    "prev_bot_share_num": 0,
                    "share_num": 0,
                    "current_investment_amount": 0,
                    "side": "string",
                    "price": 0,
                    "hedge_share": "string",
                    'stamp':0,
                    'commission':0
                    
                    }],
            response_only=True, # signal that example only applies to responses
            status_codes=[200]
        ),
    ]
)
class PerformanceSerializer(serializers.ModelSerializer):
    prev_bot_share_num = serializers.SerializerMethodField()
    side = serializers.SerializerMethodField()
    price = serializers.FloatField(source='last_live_price')
    hedge_share = serializers.SerializerMethodField()
    stamp = serializers.SerializerMethodField()
    commission= serializers.SerializerMethodField()

    class Meta:
        model = PositionPerformance
        fields = ('created','prev_bot_share_num','share_num','current_investment_amount','side','price','hedge_share','stamp','commission')
    
    def get_hedge_share(self, obj) -> int:
        if obj.order_summary:
            if 'hedge_shares' in obj.order_summary:
                return int(obj.order_summary['hedge_shares'])
        return 0
    
    def get_side(self, obj)-> str:
        if obj.order_uid:
            return obj.order_uid.side
        return "hold"
    
    def get_stamp(self,obj)-> float:
        if obj.order_uid:
            stamp = OrderFee.objects.filter(order_uid=obj.order_uid, fee_type=f'{obj.order_uid.side} stamp_duty fee')
            if stamp.exists() and stamp.count() == 1:
                return stamp.get().amount
        return 0

    def get_commission(self,obj)-> float:
        if obj.order_uid:
            stamp = OrderFee.objects.filter(order_uid=obj.order_uid, fee_type=f'{obj.order_uid.side} commissions fee')
            if stamp.exists() and stamp.count() == 1:
                return stamp.get().amount
        return 0

    
    def get_prev_bot_share_num(self, obj)-> int:
        prev = PositionPerformance.objects.filter(
            position_uid=obj.position_uid, created__lt=obj.created).order_by('created').last()
        if prev:
            return int(prev.share_num)
        return 0

class PositionSerializer(serializers.ModelSerializer):
    option_type = serializers.SerializerMethodField()
    stock_name = serializers.SerializerMethodField()
    last_price = serializers.SerializerMethodField()
    stamp = serializers.SerializerMethodField()
    commission= serializers.SerializerMethodField()
    total_fee= serializers.SerializerMethodField()
    turnover= serializers.SerializerMethodField()
    class Meta:
        model = OrderPosition
        exclude =("commision_fee","commision_fee_sell")

    def get_turnover(self,obj)-> float:
        total =0
        perf = obj.order_position.all().order_by('created').values('last_live_price','share_num')
        for index,item in enumerate(perf):
            if index == 0:
                prev_share = 0
            else:
                prev_share =perf[index-1]['share_num']
            turn_over = item['last_live_price']*abs(item['share_num']- prev_share)
            total += turn_over
        return total


    def get_stamp(self,obj)-> float:
        transaction=TransactionHistory.objects.filter(
            transaction_detail__event='stamp_duty',transaction_detail__position=obj.position_uid).aggregate(total=Sum('amount'))
        if transaction['total']:
            result = round(transaction['total'], 2)
            return result
        return 0
    def get_commission(self,obj)-> float:
        transaction=TransactionHistory.objects.filter(
            transaction_detail__event='fee',transaction_detail__position=obj.position_uid).aggregate(total=Sum('amount'))
        if transaction['total']:
            result = round(transaction['total'], 2)
            return result
        return 0
    
    def get_total_fee(self,obj)-> float:
        transaction=TransactionHistory.objects.filter(
            transaction_detail__event__in=['fee','stamp_duty'],transaction_detail__position=obj.position_uid).aggregate(total=Sum('amount'))
        if transaction['total']:
            result = round(transaction['total'], 2)
            return result
        return 0


    def get_option_type(self,obj) -> str:
        return obj.bot.bot_option_type
    
    def get_stock_name(self,obj)  -> str:
        if obj.ticker.ticker_name:
            return obj.ticker.ticker_name
        return obj.ticker.ticker_fullname
        
    
    def get_last_price(self,obj) -> float:
        return obj.ticker.latest_price_ticker.close



