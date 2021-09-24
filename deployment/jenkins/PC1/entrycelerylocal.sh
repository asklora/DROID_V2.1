#!/bin/bash

python manage.py createuser -e chariot_sm_usd_classic@chariot.com -p chariotadmin -c USD -amt 5000 -cap small -type CLASSIC -s bot_tester -b2b -client CHARIOT --settings=config.settings.production
python manage.py createuser -e chariot_lg_usd_classic@chariot.com -p chariotadmin -c USD -amt 30000 -cap large -type CLASSIC -s bot_tester -b2b -client CHARIOT --settings=config.settings.production
python manage.py createuser -e chariot_sm_usd_uno@chariot.com -p chariotadmin -c USD -amt 5000 -cap small -type UNO -s bot_tester -b2b -client CHARIOT --settings=config.settings.production
python manage.py createuser -e chariot_lg_usd_uno@chariot.com -p chariotadmin -c USD -amt 30000 -cap large -type UNO -s bot_tester -b2b -client CHARIOT --settings=config.settings.production
python manage.py createuser -e chariot_sm_usd_ucdc@chariot.com -p chariotadmin -c USD -amt 5000 -cap small -type UCDC -s bot_tester -b2b -client CHARIOT --settings=config.settings.production
python manage.py createuser -e chariot_lg_usd_ucdc@chariot.com -p chariotadmin -c USD -amt 30000 -cap large -type UCDC -s bot_tester -b2b -client CHARIOT --settings=config.settings.production
python manage.py createuser -e chariot_sm_krw_classic@chariot.com -p chariotadmin -c krw -amt 5000000 -cap small -type CLASSIC -s bot_tester -b2b -client CHARIOT --settings=config.settings.production
python manage.py createuser -e chariot_lg_krw_classic@chariot.com -p chariotadmin -c krw -amt 30000000 -cap large -type CLASSIC -s bot_tester -b2b -client CHARIOT --settings=config.settings.production
python manage.py createuser -e chariot_sm_krw_uno@chariot.com -p chariotadmin -c krw -amt 5000000 -cap small -type UNO -s bot_tester -b2b -client CHARIOT --settings=config.settings.production
python manage.py createuser -e chariot_lg_krw_uno@chariot.com -p chariotadmin -c krw -amt 30000000 -cap large -type UNO -s bot_tester -b2b -client CHARIOT --settings=config.settings.production
python manage.py createuser -e chariot_sm_krw_ucdc@chariot.com -p chariotadmin -c krw -amt 5000000 -cap small -type UCDC -s bot_tester -b2b -client CHARIOT --settings=config.settings.production
python manage.py createuser -e chariot_lg_krw_ucdc@chariot.com -p chariotadmin -c krw -amt 30000000 -cap large -type UCDC -s bot_tester -b2b -client CHARIOT --settings=config.settings.production



