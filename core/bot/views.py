from rest_framework import views,response,status
from .serializers import BotHedgerSerializer





class BotHedgerViews(views.APIView):
    serializer_class =BotHedgerSerializer


    def post(self,request):
        serializer = BotHedgerSerializer(data=request.data)
        if serializer.is_valid():
            serializer.save()
            return response.Response(serializer.data,status=status.HTTP_200_OK)
        return response.Response(serializer.errors,status=status.HTTP_400_BAD_REQUEST)