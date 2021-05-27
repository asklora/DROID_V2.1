.PHONY: build

training:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --training True

restart_server:
	@/sbin/shutdown -r now
	
stop_python:
	@sudo pkill python

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

worldscope_jpy:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --worldscope True --currency_code JPY

worldscope_krw:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --worldscope True --currency_code KRW

worldscope_cny:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --worldscope True --currency_code CNY

worldscope_mix:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --worldscope True --currency_code SGD GBP HKD

worldscope_twd:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --worldscope True --currency_code TWD

worldscope_eur:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --worldscope True --currency_code EUR

worldscope_usd:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --worldscope True --currency_code USD

fundamentals_score_jpy:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --fundamentals_score True --currency_code JPY

fundamentals_score_krw:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --fundamentals_score True --currency_code KRW

fundamentals_score_cny:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --fundamentals_score True --currency_code CNY

fundamentals_score_mix:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --fundamentals_score True --currency_code SGD GBP HKD

fundamentals_score_twd:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --fundamentals_score True --currency_code TWD

fundamentals_score_eur:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --fundamentals_score True --currency_code EUR

fundamentals_score_usd:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --fundamentals_score True --currency_code USD

quandl:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --quandl True

vix:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --vix True

interest:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --interest True

dividend:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --dividend True

utc_offset:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --utc_offset True

west_market:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --ws True

north_asia_market:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --na True

weekly:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --na True

monthlu:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --na True