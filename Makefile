.PHONY: build

training:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --training True

restart_server:
	@/sbin/shutdown -r now
	
stop_python:
	@sudo pkill python

classic:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --classic True

usd_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --prep True --currency_code USD

usd_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code USD --uno True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --backtest True --ranking True --statistic True --currency_code USD

usd_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code USD --ucdc True

krw_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --prep True --do_infer True --currency_code KRW --infer True

krw_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code KRW --uno True --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --backtest True --ranking True --statistic True --currency_code KRW

krw_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code KRW --ucdc True --infer True

hkd_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --prep True --do_infer True --currency_code HKD --infer True

hkd_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code HKD --uno True --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --backtest True --ranking True --statistic True --currency_code HKD

hkd_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code HKD --ucdc True --infer True

eur_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --prep True --do_infer True --currency_code EUR --infer True

eur_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code EUR --uno True --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --backtest True --ranking True --statistic True --currency_code EUR

eur_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code EUR --ucdc True --infer True

cny_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --prep True --do_infer True --currency_code CNY --infer True

cny_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code CNY --uno True --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --backtest True --ranking True --statistic True --currency_code CNY

cny_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code CNY --ucdc True --infer True

sgd_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --prep True --do_infer True --currency_code SGD --infer True

sgd_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code SGD --uno True --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --backtest True --ranking True --statistic True --currency_code SGD

sgd_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code SGD --ucdc True --infer True

twd_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --prep True --do_infer True --currency_code TWD --infer True

twd_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code TWD --uno True --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --backtest True --ranking True --statistic True --currency_code TWD

twd_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code TWD --ucdc True --infer True

jpy_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --prep True --do_infer True --currency_code JPY --infer True

jpy_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code JPY --uno True --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --backtest True --ranking True --statistic True --currency_code JPY

jpy_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code JPY --ucdc True --infer True

gbp_prep:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --prep True --do_infer True --currency_code GBP --infer True

gbp_uno:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code GBP --uno True --infer True
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --backtest True --ranking True --statistic True --currency_code GBP

gbp_ucdc:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py backtest --settings=config.production --option_maker True --null_filler True --currency_code GBP --ucdc True --infer True

worldscope_jpy:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --worldscope True --currency_code JPY

worldscope_krw:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --worldscope True --currency_code KRW

worldscope_cny:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --worldscope True --currency_code CNY

worldscope_mix:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --worldscope True --currency_code SGD GBP HKD

worldscope_twd:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --worldscope True --currency_code TWD

worldscope_eur:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --worldscope True --currency_code EUR

worldscope_usd:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --worldscope True --currency_code USD

fundamentals_score_jpy:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --fundamentals_score True --currency_code JPY

fundamentals_score_krw:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --fundamentals_score True --currency_code SGD GBP KRW

fundamentals_score_cny:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --fundamentals_score True --currency_code CNY

fundamentals_score_mix:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --fundamentals_score True --currency_code HKD

fundamentals_score_twd:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --fundamentals_score True --currency_code TWD

fundamentals_score_eur:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --fundamentals_score True --currency_code EUR

fundamentals_score_usd:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --fundamentals_score True --currency_code USD

quandl:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --quandl True

vix:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --vix True

interest:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --interest True

dividend:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --dividend True

utc_offset:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --utc_offset True

west_market:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --ws True

north_asia_market:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --na True

weekly:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --weekly True

monthly:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.production --monthly True