from rest_framework import serializers
from .models import OrderPosition



class PerformanceSerializer(serializers.ModelSerializer):
    pass

class PositionSerializer(serializers.ModelSerializer):
    

    class Meta:
        model = OrderPosition
        fields = "__all__"