from rest_framework import response, status
from rest_framework.views import APIView
from core.services.serializers import BroadcastSenderSerializer


class BroadcastSender(APIView):
    serializer_class = BroadcastSenderSerializer
    authentication_classes = []

    def post(self, request):
        serializer = BroadcastSenderSerializer(data=request.data)
        key = request.META.get("HTTP_CHECK_KEY")
        if serializer.is_valid():
            try:
                if key == "sendmessage":
                    serializer.save()
                else:
                    return response.Response(
                        {"message": "Check your key"},
                        status=status.HTTP_403_FORBIDDEN,
                    )
            except Exception as e:
                return response.Response(
                    {"message": str(e)},
                    status=status.HTTP_400_BAD_REQUEST,
                )
            return response.Response(
                {"message": "Message sent successfully"},
                status=status.HTTP_200_OK,
            )
        return response.Response(
            {"message": "Invalid value"},
            status=status.HTTP_400_BAD_REQUEST,
        )
