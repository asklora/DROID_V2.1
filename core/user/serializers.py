from rest_framework import serializers
from .models import User

class UserSerializer(serializers.ModelSerializer):
    password = serializers.CharField(write_only=True)

    class Meta:
        model = User
        # fields = '__all__'
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
    current_total_invested_amount=serializers.FloatField(read_only=True)
    total_amount=serializers.FloatField(read_only=True)
    total_profit_amount=serializers.FloatField(read_only=True)
    total_profit_return=serializers.FloatField(read_only=True)
    
    
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
            'current_total_invested_amount',
            'total_amount',
            'total_profit_amount',
            'total_profit_return',
            'currency',
)
    