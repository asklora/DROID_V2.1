from rest_framework import serializers
from .models import Client,UserClient
from core.user.models import User
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiExample,extend_schema_serializer



@extend_schema_serializer(
    examples = [
         OpenApiExample(
            'Return List of Users',
            description='Return List of Users',
            value=[{
                    "client_uid": "string",
                    "client_name": "string"
                        }],
            response_only=True, # signal that example only applies to responses
        ),
    ]
)
class ClientSerializers(serializers.ModelSerializer):
    
    
    class Meta:
        model = Client
        fields = ('client_uid','client_name')

@extend_schema_serializer(
    examples = [
         OpenApiExample(
            'Return List of Users',
            description='Return List of Users',
            value=[{
                    "email": "string",
                    "user_id": 0,
                    "extra_data": {
                        "property1": 'string',
                        "property2": 'string'
                    }
                        }],
            response_only=True, # signal that example only applies to responses
        ),
    ]
)
class UserClientSerializers(serializers.ModelSerializer):
    email = serializers.CharField(read_only=True,source='user.email')
    


    class Meta:
        model = UserClient
        fields = ('email','user_id','extra_data')
        

