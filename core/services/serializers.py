from rest_framework import serializers

from core.services.notification import send_bulk_notification


class BroadcastSenderSerializer(serializers.Serializer):
    title = serializers.CharField(max_length=255)
    body = serializers.CharField()

    def create(self, validated_data):
        title = validated_data.get("title", None)
        body = validated_data.get("body", None)

        send_bulk_notification(title, body)

        return validated_data
