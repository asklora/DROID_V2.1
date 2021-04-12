from rest_framework import serializers


class HedgeSerializer(serializers.Serializer):
    # all field is required, otherwise will throw error 400
    bot_id = serializers.CharField(required=True)
    ticker = serializers.CharField(required=True)
    currency = serializers.CharField(required=True)
    price = serializers.FloatField(required=True)
    spot_date = serializers.DateField(required=True,format="%Y-%m-%d", input_formats=["%Y-%m-%d", "iso-8601"])
    amount = serializers.FloatField(required=True)

# {
#     "bot_id":"CLASSIC_classic_007692",
#     "ticker":"AAPL.O",
#     "currency":"USD",
#     "price":128.0,
#     "spot_date":"2021-03-8",
#     "amount":5000
# }