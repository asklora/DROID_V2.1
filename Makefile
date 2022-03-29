.PHONY: build

training:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/backtest_temp.py --training True

restart_server:
	@/sbin/shutdown -r now
	
stop_python:
	@sudo pkill python

data_migrations:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/migrations_code.py
	
data_prep:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/backtest_temp.py --data_prep True
	
classic:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/backtest_temp.py --classic True

usd0_uno:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/backtest_temp.py --currency_code USD --uno True --split 0

usd1_uno:
	@sudo /home/loratech/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/backtest_temp.py --currency_code USD --uno True --split 1

usd0_ucdc:
	@sudo /home/loratech/PycharmProjects/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/backtest_temp.py --currency_code USD --ucdc True --split 0

usd1_ucdc:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/backtest_temp.py --currency_code USD --ucdc True --split 1

cny_uno:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/backtest_temp.py --currency_code CNY --uno True --infer True

cny_ucdc:
	@sudo /home/loratech/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/backtest_temp.py --currency_code CNY --ucdc True --infer True

hkd_uno:
	@sudo /home/loratech/PycharmProjects/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/backtest_temp.py --currency_code HKD --uno True --infer True

hkd_ucdc:
	@sudo /home/loratech/PycharmProjects/DROID_V2.1/env/bin/python3 /home/loratech/PycharmProjects/DROID_V2.1/backtest_temp.py --currency_code HKD --ucdc True --infer True
