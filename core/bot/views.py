from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from .serializers import HedgeSerializer
from .models import BotOptionType
from datetime import datetime

# agustian function library
from bot.calculate_bot import (
    get_classic,
    get_ucdc,
    get_uno,
    get_expiry_date 
    )


@api_view(['POST','GET'])
def bothedgeview(request):
    # if request post run the method, else will throw response 405 not allowed
    if request.method == 'POST':
        #use serializer to validation the field data
        serializer = HedgeSerializer(data=request.data)

        #  if field value in the serializer is valid will continue the logic, otherwise will throw error 400
        if serializer.is_valid():
            bot_id = serializer.data.get('bot_id')
            # get bot option type for required argumen in bot hedge function
            try:
                bot = BotOptionType.objects.get(bot_id=bot_id)
            except BotOptionType.DoesNotExist:
                return Response({'message':f'bot with id {bot_id} not found'},status=status.HTTP_404_NOT_FOUND)
            spot_date = serializer.data.get('spot_date')
            if isinstance(spot_date,str):
                spot_date = datetime.strptime(spot_date,"%Y-%m-%d") 
            expiry_date = get_expiry_date(bot.time_to_exp, spot_date ,serializer.data.get('currency'))
            classic = get_classic(serializer.data.get('ticker'),spot_date,
                                  bot.time_to_exp,serializer.data.get('amount'),serializer.data.get('price'),expiry_date)
            print(classic)
            return Response(data=classic,status=status.HTTP_200_OK)
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST) # error handle and error message
    return Response({'message':'method not allowed'},status=status.HTTP_405_METHOD_NOT_ALLOWED) # block get request
