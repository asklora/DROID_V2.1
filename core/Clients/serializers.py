from rest_framework import serializers
from .models import Client



class ClientSerializers(serializers.ModelSerializer):
    
    
    class Meta:
        model = Client
        fields = ('client_uid','client_name')
        

