.PHONY: build

training:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --training True

restart_server:
	@/sbin/shutdown -r now
	
stop_python:
	@sudo pkill python

classic:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --classic True

cny_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --prep True --do_infer True --currency_code CNY --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --prep True --currency_code CNY

cny_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code CNY --uno True --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --backtest True --ranking True --statistic True --currency_code CNY

cny_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code CNY --ucdc True --infer True

krw_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --prep True --do_infer True --currency_code KRW --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --prep True --currency_code KRW

krw_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code KRW --uno True --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --backtest True --ranking True --statistic True --currency_code KRW

krw_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code KRW --ucdc True --infer True

hkd_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --prep True --do_infer True --currency_code HKD --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --prep True --currency_code HKD

hkd_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code HKD --uno True --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --backtest True --ranking True --statistic True --currency_code HKD

hkd_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code HKD --ucdc True --infer True

usd_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --prep True --currency_code USD

usd_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code USD --uno True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --backtest True --ranking True --statistic True --currency_code USD

usd_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code USD --ucdc True

eur_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --prep True --do_infer True --currency_code EUR --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --prep True --currency_code EUR

eur_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code EUR --uno True --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --backtest True --ranking True --statistic True --currency_code EUR

eur_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code EUR --ucdc True --infer True

gbp_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --prep True --do_infer True --currency_code GBP --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --prep True --currency_code GBP

gbp_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code GBP --uno True --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --backtest True --ranking True --statistic True --currency_code GBP

gbp_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code GBP --ucdc True --infer True

firebase_universe_na:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --na True --firebase_universe True

firebase_universe_ws:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --ws True --firebase_universe True

sgd_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --prep True --do_infer True --currency_code SGD --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --prep True --currency_code SGD

sgd_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code SGD --uno True --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --backtest True --ranking True --statistic True --currency_code SGD

sgd_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code SGD --ucdc True --infer True

twd_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --prep True --do_infer True --currency_code TWD --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --prep True --currency_code TWD

twd_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code TWD --uno True --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --backtest True --ranking True --statistic True --currency_code TWD

twd_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code TWD --ucdc True --infer True

jpy_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --prep True --do_infer True --currency_code JPY --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --prep True --currency_code JPY

jpy_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code JPY --uno True --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --backtest True --ranking True --statistic True --currency_code JPY

jpy_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.settings.production --option_maker True --null_filler True --currency_code JPY --ucdc True --infer True
