import socket

import pandas as pd
import pytest
from core.universe.models import Currency
from core.user.models import Accountbalance, User, UserProfitHistory
from django.core.management import call_command


pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


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

    # new users will have 0 in their balance
    assert data["deposit"].iloc[-1] == 0.0


def test_user_deposit_history(mocker) -> None:
    computer_name = socket.gethostname().lower()
    unique_email = f"{computer_name}-deposit@tests.com"

    # We get our user here
    new_user = User.objects.create(
        email=unique_email,
        username=computer_name,
        is_active=True,
        current_status="verified",
    )
    new_user.set_password("everything_is_but_a_test")
    new_user.save()

    Accountbalance.objects.create(
        user=new_user,
        amount=0,
        currency_code_id="HKD",
    )

    # we mock the database queries with our own
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
    new_user.is_joined = True
    new_user.save()

    # we make sure that our mock functions are called
    # and run exactly once
    user_core_mock.assert_called_once()
    user_balance_mock.assert_called_once()
    user_profit_mock.assert_called_once()
    currency_mock.assert_called_once()
    upsert_mock.assert_called_once()

    call_command("delete_user", username=new_user.username)
