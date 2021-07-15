from rest_framework import serializers
from core.orders.models import Order,OrderPosition,PositionPerformance
from core.Clients.models import ClientTopStock
from datetime import datetime
from core.master.models import MasterOhlcvtr,LatestPrice
from core.universe.models import Universe


class DetailUniverse(serializers.ModelSerializer):

    industry_name=serializers.CharField(source='industry_code.industry_name',read_only=True)
    industry_group_code=serializers.CharField(source='industry_code.industry_group_code.industry_group_code',read_only=True)
    industry_group_name=serializers.CharField(source='industry_code.industry_group_code.industry_group_name',read_only=True)


    class Meta:
        model = Universe
        fields = [
            'currency_code',
            'industry_name',
            'industry_group_code',
            'industry_group_name',
            'industry_code',
            'wc_industry_code',
            'quandl_symbol',
            'ticker_name',
            'ticker_fullname',
            'company_description',
            'ticker_symbol',
            'mic'
        ]


class PriceUniverse(serializers.ModelSerializer):
    revenue_per_share=serializers.FloatField(source='ticker.revenue_per_share',read_only=True)
    market_cap=serializers.FloatField(source='ticker.market_cap',read_only=True)
    pe_ratio=serializers.FloatField(source='ticker.pe_ratio',read_only=True)
    pe_forecast=serializers.FloatField(source='ticker.pe_forecast',read_only=True)
    pb=serializers.FloatField(source='ticker.pb',read_only=True)
    ebitda=serializers.FloatField(source='ticker.ebitda',read_only=True)
    wk52_high=serializers.FloatField(source='ticker.wk52_high',read_only=True)
    wk52_low=serializers.FloatField(source='ticker.wk52_low',read_only=True)
    free_cash_flow=serializers.FloatField(source='ticker.free_cash_flow',read_only=True)

    class Meta:
        model = LatestPrice
        fields = [
            'revenue_per_share',
            'market_cap',
            'pe_ratio',
            'pe_forecast',
            'pb',
            'ebitda',
            'wk52_high',
            'wk52_low',
            'free_cash_flow',
            'open',
            'high',
            'low',
            'close',
            'intraday_date',
            'intraday_ask',
            'intraday_bid',
            'latest_price_change',
            'intraday_time',
            'last_date',
            'capital_change',
            'volume',
            'latest_price'
        ]


class HotUniverse(serializers.ModelSerializer):
    detail = serializers.SerializerMethodField()
    price = serializers.SerializerMethodField()

    class Meta:
        model = Universe
        fields = ['ticker','detail','price']

    def get_detail(self,obj):
        return DetailUniverse(obj).data
    
    
    def get_price(self,obj):
        return PriceUniverse(LatestPrice.objects.get(ticker=obj)).data








class OrderSerializer(serializers.ModelSerializer):
    class Meta:
        fields='__all__'
        model=Order


class OrderPositionSerializer(serializers.ModelSerializer):
    class Meta:
        fields='__all__'
        model=OrderPosition


class PositionPerformanceSerializer(serializers.ModelSerializer):
    class Meta:
        fields='__all__'
        model=PositionPerformance
        
class CsvSerializer(serializers.ModelSerializer):
    bot_id = serializers.CharField(source="position_uid.bot_id")
    bot_status = serializers.SerializerMethodField()
    side = serializers.SerializerMethodField()
    position_id = serializers.CharField(source="position_uid.position_uid")
    ticker = serializers.CharField(source="position_uid.ticker.ticker")
    uuid = serializers.CharField(source="performance_uid")
    bot_share_num = serializers.IntegerField(source="share_num")
    delta = serializers.FloatField(source="last_hedge_delta")
    prev_delta = serializers.SerializerMethodField()
    prev_bot_share_num = serializers.SerializerMethodField()
    hedge_shares = serializers.SerializerMethodField()
    max_loss_price = serializers.FloatField(
        source="position_uid.max_loss_price")
    max_loss_pct = serializers.FloatField(source="position_uid.max_loss_pct")
    target_profit_price = serializers.FloatField(
        source="position_uid.target_profit_price")
    target_profit_pct = serializers.FloatField(
        source="position_uid.target_profit_pct")
    expired_in = serializers.CharField(
        source="position_uid.expiry")
    price = serializers.FloatField(
        source="last_live_price")
    entry_price = serializers.FloatField(
        source="last_spot_price")
    option_type = serializers.CharField(source="position_uid.bot.bot_type")
    service_type = serializers.SerializerMethodField()
    capital = serializers.SerializerMethodField()
    currency = serializers.CharField(
        source="position_uid.ticker.currency_code")
    index = serializers.SerializerMethodField()
    index_price = serializers.SerializerMethodField()
    
     

    class Meta:
        model = PositionPerformance
        fields = ("service_type", "capital", "bot_id", "option_type", "created",
                  "side", "bot_status", "ticker", "currency",  "current_investment_amount",
                  "entry_price", "price", "max_loss_price", "max_loss_pct",
                  "target_profit_price",
                  "target_profit_pct",
                  "prev_bot_share_num", "bot_share_num",  "hedge_shares", "prev_delta", "delta",
                  "strike", "v1", "v2",
                  "barrier", "expired_in",
                  "uuid", "position_id",'index','index_price'

                  )

    def get_service_type(self, obj):
        top_stock = ClientTopStock.objects.filter(
            position_uid=obj.position_uid.position_uid)
        if top_stock.exists():
            top_stock = top_stock.get()
            return top_stock.service_type
        return "-"

    def get_capital(self, obj):
        top_stock = ClientTopStock.objects.filter(
            position_uid=obj.position_uid.position_uid)
        if top_stock.exists():
            top_stock = top_stock.get()
            return top_stock.capital
        return "-"

    def get_hedge_shares(self, obj):
        prev = PositionPerformance.objects.filter(
            position_uid=obj.position_uid, created__lt=obj.created).order_by('created').last()
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
            position_uid=obj.position_uid, created__lt=obj.created).order_by('created').last()
        if prev:
            return prev.last_hedge_delta
        return 0

    def get_prev_bot_share_num(self, obj):
        prev = PositionPerformance.objects.filter(
            position_uid=obj.position_uid, created__lt=obj.created).order_by('created').last()
        if prev:
            return int(prev.share_num)
        return 0

    def get_side(self, obj):
        if obj.order_uid:
            return obj.order_uid.side
        return "hold"

    def get_bot_status(self, obj):
        if obj.order_uid:
            if obj.order_uid.is_init:
                return "new"
            elif not obj.position_uid.is_live and obj.position_uid.event_date == obj.created.date():
                return obj.position_uid.event
            else:
                return "live"
        elif obj.order_uid and not obj.position_uid.is_live and obj.position_uid.event_date == obj.created.date():
            return obj.position_uid.event
        else:
            return "live"
        
    def get_index(self, obj):
        return obj.position_uid.ticker.currency_code.index_ticker
    
    
    def get_index_price(self, obj):
        if obj.created.date() == datetime.now().date():
            return obj.position_uid.ticker.currency_code.index_price
        price = MasterOhlcvtr.objects.get(trading_day=obj.created,ticker=obj.position_uid.ticker.currency_code.index_ticker)
        return price.close