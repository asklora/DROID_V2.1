from rest_framework import serializers
from core.orders.models import Order,OrderPosition,PositionPerformance


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