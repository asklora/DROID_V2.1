.PHONY: build

restart_server:
	@/sbin/shutdown -r now
	
stop_python:
	@sudo pkill python

migrations:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py migrations --weekly True

classic:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --classic True

usd_prep:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --prep True --currency_code USD

usd_uno:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --option_maker True --null_filler True --currency_code USD --uno True
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --backtest True --ranking True --currency_code USD
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --statistic True

usd_ucdc:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --option_maker True --null_filler True --currency_code USD --ucdc True

krw_prep:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --prep True --do_infer True --currency_code KRW --infer True

krw_uno:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --option_maker True --null_filler True --currency_code KRW --uno True --infer True
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --backtest True --ranking True --currency_code KRW

krw_ucdc:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --option_maker True --null_filler True --currency_code KRW --ucdc True --infer True

hkd_prep:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --prep True --do_infer True --currency_code HKD --infer True

hkd_uno:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --option_maker True --null_filler True --currency_code HKD --uno True --infer True

hkd_ucdc:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --option_maker True --null_filler True --currency_code HKD --ucdc True --infer True

eur_prep:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --prep True --do_infer True --currency_code EUR --infer True

eur_uno:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --option_maker True --null_filler True --currency_code EUR --uno True --infer True
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --backtest True --ranking True --currency_code EUR

eur_ucdc:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --option_maker True --null_filler True --currency_code EUR --ucdc True --infer True

cny_prep:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --prep True --do_infer True --currency_code CNY --infer True

cny_uno:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --option_maker True --null_filler True --currency_code CNY --uno True --infer True
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --backtest True --ranking True --currency_code CNY

cny_ucdc:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --option_maker True --null_filler True --currency_code CNY --ucdc True --infer True

training:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --training True
