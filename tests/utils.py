from datetime import datetime

import pandas as pd
from core.orders.models import Order, OrderPosition, PositionPerformance
from core.universe.models import Currency
from core.user.models import (Accountbalance, TransactionHistory, User,
                              UserProfitHistory)
from django.utils import timezone
from general.firestore_query import delete_firestore_user
from ingestion import firebase_user_update


def get_user_core(currency_code=None, user_id=None, field="*") -> pd.DataFrame:
    users = User.objects.filter(pk=user_id[0])
    users_df = pd.DataFrame(list(users.values()))[["id", "is_joined"]]
    users_df = users_df.rename(columns={"id": "user_id"})

    return users_df


def get_user_account_balance(
    currency_code=None,
    user_id=None,
    field="*",
) -> pd.DataFrame:
    account_balances = Accountbalance.objects.filter(user_id=user_id[0])
    account_balances_df = pd.DataFrame(list(account_balances.values()))[
        ["user_id", "amount", "currency_code_id"]
    ]
    account_balances_df = account_balances_df.rename(
        columns={
            "amount": "balance",
            "currency_code_id": "currency_code",
        }
    )

    return account_balances_df


def get_user_profit_history(user_id=None, field="*") -> pd.DataFrame:
    # Newly joined user should not have  profit history data
    assert UserProfitHistory.objects.filter(user_id=user_id[0]).count() == 0

    # for the dataframe, we return empty dataframe with columns
    # similar to UserProfitHistory
    profit_history_col = [f.name for f in UserProfitHistory._meta.get_fields()]
    profit_history_df = pd.DataFrame(columns=profit_history_col)
    profit_history_df = profit_history_df[["user_id", "daily_invested_amount"]]

    return profit_history_df


def get_currency_data(currency_code=None) -> pd.DataFrame:
    currency = Currency.objects.all()
    currency_df = pd.DataFrame(list(currency.values()))

    return currency_df


def upsert_data_to_database(
    data,
    table,
    primary_key,
    how="update",
    cpu_count=False,
    Text=False,
    Date=False,
    Int=False,
    Bigint=False,
    Bool=False,
    debug=False,
) -> None:
    print("\n" + table)
    print(data)


def set_user_joined(mocker, user: User) -> None:
    user_core_mock = mocker.patch(
        "bot.calculate_bot.get_user_core",
        wraps=get_user_core,
    )
    user_balance_mock = mocker.patch(
        "bot.calculate_bot.get_user_account_balance",
        wraps=get_user_account_balance,
    )
    user_profit_mock = mocker.patch(
        "bot.calculate_bot.get_user_profit_history",
        wraps=get_user_profit_history,
    )
    currency_mock = mocker.patch(
        "bot.calculate_bot.get_currency_data",
        wraps=get_currency_data,
    )
    upsert_mock = mocker.patch(
        "bot.calculate_bot.upsert_data_to_database",
        wraps=upsert_data_to_database,
    )

    # we set the new user as joined
    # to trigger the user deposit history table update
    user.is_joined = True
    user.save()

    # we make sure that our mock functions are called
    # and run exactly once
    user_core_mock.assert_called_once()
    user_balance_mock.assert_called_once()
    user_profit_mock.assert_called_once()
    currency_mock.assert_called_once()
    upsert_mock.assert_called_once()

    # we manually have to put it here because in the server,
    # this part of code is called when the user joined
    firebase_user_update([user.id])


def delete_user(user: User) -> None:
    user_id = user.id

    PositionPerformance.objects.filter(position_uid__user_id=user).delete()
    Order.objects.filter(user_id=user).delete()
    OrderPosition.objects.filter(user_id=user).delete()
    TransactionHistory.objects.filter(balance_uid__user=user).delete()
    Accountbalance.objects.filter(user=user).delete()
    user.delete()
    delete_firestore_user(user_id)


def create_buy_order(
    price: float,
    ticker: str,
    amount: float = None,
    bot_id: str = "STOCK_stock_0",
    created: datetime = timezone.now(),
    margin: int = 1,
    qty: int = 100,
    user_id: int = None,
    user: User = None,
) -> Order:
    return Order.objects.create(
        amount=amount if amount is not None else price * qty,
        bot_id=bot_id,
        created=created,
        margin=margin,
        order_type="apps",  # to differentiate itself from FELS's orders
        price=price,
        qty=qty,
        side="buy",
        ticker_id=ticker,
        user_id_id=user_id,
        user_id=user,
    )
