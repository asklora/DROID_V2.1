from rest_framework import serializers
from .models import OrderPosition, PositionPerformance
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiExample,extend_schema_serializer


@extend_schema_serializer(
    examples = [
         OpenApiExample(
            'Get bot Performance by positions',
            description='Get bot Performance by positions',
            value=[{
                    "created": "2021-05-27T05:25:55.776Z",
                    "prev_bot_share_num": "string",
                    "share_num": 0,
                    "current_investment_amount": 0,
                    "side": "string",
                    "price": 0,
                    "hedge_share": "string"
                    }],
            response_only=True, # signal that example only applies to responses
        ),
    ]
)
class PerformanceSerializer(serializers.ModelSerializer):
    prev_bot_share_num = serializers.SerializerMethodField()
    side = serializers.SerializerMethodField()
    price = serializers.FloatField(source='last_live_price')
    hedge_share = serializers.SerializerMethodField()

    class Meta:
        model = PositionPerformance
        fields = ('created','prev_bot_share_num','share_num','current_investment_amount','side','price','hedge_share')
    
    def get_hedge_share(self, obj):
        if obj.order_summary:
            if 'hedge_shares' in obj.order_summary:
                return int(obj.order_summary['hedge_shares'])
        return 0
    
    def get_side(self, obj):
        if obj.order_uid:
            return obj.order_uid.side
        return "hold"
    
    
    def get_prev_bot_share_num(self, obj):
        prev = PositionPerformance.objects.filter(
            position_uid=obj.position_uid, created__lt=obj.created).order_by('created').last()
        if prev:
            return int(prev.share_num)
        return 0

class PositionSerializer(serializers.ModelSerializer):
    

    class Meta:
        model = OrderPosition
        
        fields = "__all__"