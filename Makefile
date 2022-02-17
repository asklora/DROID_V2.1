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

worldscope:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --worldscope True --split 16

#worldscope_2:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --worldscope True --currency_code KRW --split 1
#
#worldscope_3:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --worldscope True --currency_code HKD --split 1
#
#worldscope_4:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --worldscope True --currency_code HKD --split 2
#
#worldscope_5:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --worldscope True --currency_code HKD --split 3
#
#worldscope_6:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --worldscope True --currency_code EUR --split 1
#
#worldscope_7:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --worldscope True --currency_code EUR --split 2
#
#worldscope_8:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --worldscope True --currency_code EUR --split 3
#
#worldscope_9:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --worldscope True --currency_code CNY --split 1
#
#worldscope_10:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --worldscope True --currency_code CNY --split 2
#
#worldscope_11:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --worldscope True --currency_code CNY --split 3
#
#worldscope_12:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --worldscope True --currency_code USD --split 1
#
#worldscope_13:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --worldscope True --currency_code USD --split 2
#
#worldscope_14:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --worldscope True --currency_code USD --split 3
#
#worldscope_15:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --worldscope True --currency_code USD --split 4
#
#worldscope_16:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --worldscope True --currency_code USD --split 5

fundamentals:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --fundamentals_score True --split 16

#fundamentals_2:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --fundamentals_score True --currency_code KRW --split 1
#
#fundamentals_3:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --fundamentals_score True --currency_code HKD --split 1
#
#fundamentals_4:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --fundamentals_score True --currency_code HKD --split 2
#
#fundamentals_5:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --fundamentals_score True --currency_code HKD --split 3
#
#fundamentals_6:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --fundamentals_score True --currency_code EUR --split 1
#
#fundamentals_7:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --fundamentals_score True --currency_code EUR --split 2
#
#fundamentals_8:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --fundamentals_score True --currency_code EUR --split 3
#
#fundamentals_9:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --fundamentals_score True --currency_code CNY --split 1
#
#fundamentals_10:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --fundamentals_score True --currency_code CNY --split 2
#
#fundamentals_11:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --fundamentals_score True --currency_code CNY --split 3
#
#fundamentals_12:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --fundamentals_score True --currency_code USD --split 1
#
#fundamentals_13:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --fundamentals_score True --currency_code USD --split 2
#
#fundamentals_14:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --fundamentals_score True --currency_code USD --split 3
#
#fundamentals_15:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --fundamentals_score True --currency_code USD --split 4
#
#fundamentals_16:
#	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --fundamentals_score True --currency_code USD --split 5

fundamentals_monthly_factor:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --fundamentals_rating True

quandl:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --quandl True

vix:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --vix True

interest:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --interest True

dividend:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --dividend True

utc_offset:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --utc_offset True

currency_price:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --currency_price True

west_market:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --ws True

north_asia_market:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --na True

ai_rating_na:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --na True --ai_rating True

ai_rating_ws:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --ws True --ai_rating True

firebase_universe_na:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --na True --firebase_universe True

firebase_universe_ws:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --ws True --firebase_universe True

weekly:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --weekly True

monthly:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py main --settings=config.settings.production --monthly True
	
populate_ticker:
	@sudo /home/loratech/droid2env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/manage.py populate_ticker --settings=config.settings.production

restart_pc:
	@/sbin/shutdown -r now

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

hanwha_hedging:
	@#python3 manage.py hanwha_hedging --settings=config.settings.production --ticker TSLA.O --bot_id UNO_OTM_007692 --spot_date 2021-09-30
	@#python3 manage.py hanwha_hedging --settings=config.settings.production --ticker TSLA.O --bot_id UNO_ITM_007692 --spot_date 2021-09-30
	@#python3 manage.py hanwha_hedging --settings=config.settings.production --ticker TSLA.O --bot_id UNO_OTM_007692 --spot_date 2021-12-20
	@#python3 manage.py hanwha_hedging --settings=config.settings.production --ticker TSLA.O --bot_id UNO_ITM_007692 --spot_date 2021-12-20
	@python3 manage.py hanwha_hedging --settings=config.settings.production --ticker FB.O --bot_id UNO_OTM_007692 --spot_date 2022-01-06

	@python3 manage.py hanwha_hedging --settings=config.settings.production --ticker AAPL.O --bot_id UCDC_ATM_007692 --spot_date 2021-10-29
	@python3 manage.py hanwha_hedging --settings=config.settings.production --ticker DIS --bot_id UCDC_ATM_007692 --spot_date 2021-11-17
	@python3 manage.py hanwha_hedging --settings=config.settings.production --ticker AAPL.O --bot_id UCDC_ATM_007692 --spot_date 2021-09-20

	@python3 manage.py hanwha_hedging --settings=config.settings.production --save_csv True
