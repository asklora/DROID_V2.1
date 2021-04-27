#createuser
python manage.py createuser -e classic_s_krw_bot@hanwha.asklora.ai -p hanwha -c krw -b2b -client HANWHA -amt 5000000 -cap small -type classic -s bot_advisor
#create order
python manage.py createorder --account asklora@loratechai.com -t MSFT.O -p 227.39 -d 2021-03-08 -amt 5000 -b UNO_ITM_007692