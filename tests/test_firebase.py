import time
from datetime import datetime

from ingestion import firebase_user_update
from django.conf import settings

from tests.utils import create_buy_order, schema, set_user_joined


def test_creating_new_user_should_update_firebase(
    mocker,
    request,
    firestore_client,
    user,
):
    if not user.is_joined:
        set_user_joined(mocker, user)

    time.sleep(60)

    doc_ref = firestore_client.collection(
        settings.FIREBASE_COLLECTION["portfolio"],
    ).document(
        str(user.id),
    )

    doc = doc_ref.get()

    # Whether the user data is updated to firebase
    assert doc.exists

    doc_dict = doc.to_dict()
    # assert schema.validate(doc_dict)

    print("Firebase doc: " + str(doc_dict))

    # check if the data matches
    assert doc_dict["profile"]["username"] == user.username
    assert doc_dict["profile"]["email"] == user.email


def test_order_should_be_updated_to_firebase(
    mocker,
    request,
    firestore_client,
    user,
):
    # just in case if the user is still not joined the competition
    if not user.is_joined:
        set_user_joined(mocker, user)

    # we create a new order here
    order = create_buy_order(
        price=7.13,
        ticker="6837.HK",
        margin=2,
        user_id=user.id,
        bot_id="UNO_OTM_007692",
    )

    order.status = "placed"
    order.placed = True
    order.placed_at = datetime.now()
    order.save()

    order.status = "filled"
    order.filled_at = datetime.now()
    order.save()

    # update firebase
    firebase_user_update([user.id])

    # it takes a while to propagate to firebase so give it a second
    time.sleep(30)

    doc_ref = firestore_client.collection(
        settings.FIREBASE_COLLECTION["portfolio"],
    ).document(
        str(user.id),
    )

    doc = doc_ref.get()

    assert doc.exists

    doc_dict = doc.to_dict()
    # assert schema.validate(doc_dict)
    print(doc_dict)

    assert doc_dict["active_portfolio"]
