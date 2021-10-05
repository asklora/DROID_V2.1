from core.orders.models import Order, OrderPosition, PositionPerformance
from core.user.models import Accountbalance, TransactionHistory, User
from general.firestore_query import delete_firestore_user
from ingestion import firebase_user_update
from tests.utils.mocks import (
    get_user_core,
    get_user_account_balance,
    get_user_profit_history,
    get_currency_data,
    upsert_data_to_database,
)


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
