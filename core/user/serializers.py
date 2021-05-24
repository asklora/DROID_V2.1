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