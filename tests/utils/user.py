from core.orders.models import Order, OrderPosition, PositionPerformance
from core.user.models import Accountbalance, TransactionHistory, User
from general.firestore_query import delete_firestore_user
from ingestion.firestore_migration import firebase_user_update


def set_user_joined(mocker, user: User) -> None:
    # we set the new user as joined
    # to trigger the user deposit history table update
    user.is_joined = True
    user.save()

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
    delete_firestore_user(str(user_id))
