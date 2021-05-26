from rest_framework import serializers
from .models import Client,UserClient
from core.user.models import User



class ClientSerializers(serializers.ModelSerializer):
    
    
    class Meta:
        model = Client
        fields = ('client_uid','client_name')


class UserClientSerializers(serializers.ModelSerializer):
    email = serializers.CharField(read_only=True,source='user_id.email')
    


    class Meta:
        model = UserClient
        fields = ('email','user_id','extra_data')
        

