.PHONY: build

training:
	@sudo /home/loratech/DROID_V2.1/env/bin/python3 /home/loratech/DROID_V2.1/backtest_temp.py --training True

restart_server:
	@/sbin/shutdown -r now
	
stop_python:
	@sudo pkill python

data_migrations:
	@sudo /home/loratech/DROID_V2.1/env/bin/python3 /home/loratech/DROID_V2.1/migrations_code.py
	
data_prep:
	@sudo /home/loratech/DROID_V2.1/env/bin/python3 /home/loratech/DROID_V2.1/backtest_temp.py --data_prep True
	
classic:
	@sudo /home/loratech/DROID_V2.1/env/bin/python3 /home/loratech/DROID_V2.1/backtest_temp.py --classic True

usd_uno:
	@sudo /home/loratech/DROID_V2.1/env/bin/python3 /home/loratech/DROID_V2.1/backtest_temp.py --option_maker True --null_filler True --currency_code USD --uno True

usd_ucdc:
	@sudo /home/loratech/DROID_V2.1/env/bin/python3 /home/loratech/DROID_V2.1/backtest.py --option_maker True --null_filler True --currency_code USD --ucdc True

cny_uno:
	@sudo /home/loratech/DROID_V2.1/env/bin/python3 /home/loratech/DROID_V2.1/backtest.py --option_maker True --null_filler True --currency_code CNY --uno True --infer True

cny_ucdc:
	@sudo /home/loratech/DROID_V2.1/env/bin/python3 /home/loratech/DROID_V2.1/backtest.py --option_maker True --null_filler True --currency_code CNY --ucdc True --infer True

hkd_uno:
	@sudo /home/loratech/DROID_V2.1/env/bin/python3 /home/loratech/DROID_V2.1/backtest.py --option_maker True --null_filler True --currency_code HKD --uno True --infer True

hkd_ucdc:
	@sudo /home/loratech/DROID_V2.1/env/bin/python3 /home/loratech/DROID_V2.1/backtest.py --option_maker True --null_filler True --currency_code HKD --ucdc True --infer True
