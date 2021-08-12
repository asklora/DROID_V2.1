import pandas as pd
from general.sql_query import read_query

if __name__ == "__main__":
    query = f"select * from user_account_balance order by user_id;"
    balance = read_query(query, cpu_counts=True, prints=True)
    print(balance)
    result = pd.DataFrame({"outcome2" : [] , "user_id" : []}, index=[])
    for index, row in balance.iterrows():
        user_id = row["user_id"]

        query = f"select amount as outcome2 from user_transaction ut "
        query += f"where ut.balance_uid = (select balance_uid from user_account_balance where user_id = {user_id}) and "
        query += f"transaction_detail ->> 'event' ='first deposit'"
        first_deposit2 = read_query(query, cpu_counts=True, prints=True)
        print(first_deposit2)

        query = f"select sum(investment_amount) as outcome2 from orders_position where user_id = {user_id} and is_live = False;"
        total_investment_false = read_query(query, cpu_counts=True, prints=True)
        print(total_investment_false)

        query = f"select sum(bot_cash_balance) as outcome2 from orders_position where user_id = {user_id} and is_live = False;"
        total_return = read_query(query, cpu_counts=True, prints=True)
        print(total_return)

        query = f"select sum(amount) as outcome2 from ( "
        query += f"select *, ut.transaction_detail ->> 'position' as position_uid from user_transaction ut "
        query += f"where ut.balance_uid = (select balance_uid from user_account_balance where user_id = {user_id})) result1 "
        query += f"where side='debit' and (transaction_detail ->> 'event' = 'fee') and "
        query += f"position_uid in (select position_uid from orders_position where is_live=False and user_id = {user_id}) "
        total_fee_false = read_query(query, cpu_counts=True, prints=True)
        print(total_fee_false)

        query = f"select sum(investment_amount) as outcome2 from orders_position where user_id = {user_id} and is_live = True;"
        total_investment_true = read_query(query, cpu_counts=True, prints=True)
        print(total_investment_true)

        query = f"select sum(amount) as outcome2 from ( "
        query += f"select *, ut.transaction_detail ->> 'position' as position_uid from user_transaction ut "
        query += f"where ut.balance_uid = (select balance_uid from user_account_balance where user_id = {user_id})) result1 "
        query += f"where side='debit' and (transaction_detail ->> 'event' = 'fee') and "
        query += f"position_uid in (select position_uid from orders_position where is_live=True and user_id = {user_id}) "
        total_fee_true = read_query(query, cpu_counts=True, prints=True)
        print(total_fee_true)

        right_balance = (first_deposit2 - total_investment_false - total_fee_false + total_return - total_investment_true - total_fee_true)
        right_balance["user_id"] = user_id
        print(right_balance)


        # query = f"select amount as outcome from user_transaction ut "
        # query += f"where ut.balance_uid = (select balance_uid from user_account_balance where user_id = {user_id}) and "
        # query += f"transaction_detail ->> 'event' ='first deposit'"
        # first_deposit = read_query(query, cpu_counts=True, prints=True).loc[0, "outcome"]
        # print(first_deposit)

        # query = f"select sum(amount) as outcome from ( "
        # query += f"select *, ut.transaction_detail ->> 'position' as position_uid from user_transaction ut "
        # query += f"where ut.balance_uid = (select balance_uid from user_account_balance where user_id = {user_id})) result1 "
        # query += f"where side='debit' and position_uid in (select position_uid from orders_position where is_live=False and user_id = {user_id}) "
        # total_debit_false = read_query(query, cpu_counts=True, prints=True)
        # print(total_debit_false)

        # query = f"select sum(amount) as outcome from ( "
        # query += f"select *, ut.transaction_detail ->> 'position' as position_uid from user_transaction ut "
        # query += f"where ut.balance_uid = (select balance_uid from user_account_balance where user_id = {user_id})) result1 "
        # query += f"where side='debit' and (transaction_detail ->> 'event' = 'fee' or transaction_detail ->> 'event' = 'stamp_duty') and "
        # query += f"position_uid in (select position_uid from orders_position where is_live=False and user_id = {user_id}) "
        # total_fee_false = read_query(query, cpu_counts=True, prints=True)
        # print(total_fee_false)

        # query = f"select sum(amount) as outcome from ( "
        # query += f"select *, ut.transaction_detail ->> 'position' as position_uid from user_transaction ut "
        # query += f"where ut.balance_uid = (select balance_uid from user_account_balance where user_id = {user_id})) result1 "
        # query += f"where side='credit' and position_uid in (select position_uid from orders_position where is_live=False and user_id = {user_id}) "
        # total_credit_false = read_query(query, cpu_counts=True, prints=True)
        # print(total_credit_false)

        # query = f"select sum(amount) as outcome from ( "
        # query += f"select *, ut.transaction_detail ->> 'position' as position_uid from user_transaction ut "
        # query += f"where ut.balance_uid = (select balance_uid from user_account_balance where user_id = {user_id})) result1 "
        # query += f"where position_uid in (select position_uid from orders_position where is_live=True and user_id = {user_id}); "
        # total_debit_true = read_query(query, cpu_counts=True, prints=True)
        # print(total_debit_true)

        # right_balance = (first_deposit - total_debit_false + total_credit_false - total_debit_true)
        # right_balance["user_id"] = user_id
        # print(right_balance)
        result = result.append(right_balance)
    print(result)
    balance = balance.merge(result, how="left", on=["user_id"])
    print(balance)
    balance.to_csv("balance.csv")