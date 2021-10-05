import pandas as pd
from core.universe.models import Currency
from core.user.models import Accountbalance, User, UserProfitHistory


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
