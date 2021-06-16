from rest_framework import serializers
from .models import User

class UserSerializer(serializers.ModelSerializer):
    password = serializers.CharField(write_only=True)

    class Meta:
        model = User
        # fields = '__all__'o
        exclude = ('groups','user_permissions')


class TokenRevokeSerializer(serializers.Serializer):
    token = serializers.CharField(required=True)
    
    
class UserSummarySerializer(serializers.ModelSerializer):
    current_assets=serializers.FloatField(read_only=True)
    balance=serializers.FloatField(read_only=True)
    position_live=serializers.FloatField(read_only=True)
    position_expired=serializers.FloatField(read_only=True)
    total_invested_amount=serializers.FloatField(read_only=True)
    starting_amount=serializers.FloatField(read_only=True)
    current_total_investment_value=serializers.FloatField(read_only=True)
    total_amount=serializers.FloatField(read_only=True)
    total_profit_amount=serializers.FloatField(read_only=True)
    total_profit_return=serializers.FloatField(read_only=True)
    total_fee_amount=serializers.FloatField(read_only=True)
    total_stock_amount=serializers.FloatField(read_only=True)
    total_stamp_amount=serializers.FloatField(read_only=True)
    total_commission_amount=serializers.FloatField(read_only=True)
    
    class Meta:
        model= User
        fields=(
            'id',
            'email',
            'current_assets',
            'balance',
            'position_live',
            'position_expired',
            'total_invested_amount',
            'starting_amount',
            'current_total_investment_value',
            'total_amount',
            'total_profit_amount',
            'total_profit_return',
            'total_stock_amount',
            'currency',
            'total_fee_amount',
            'total_stamp_amount',
            'total_commission_amount',
)
    