import time

from tests.utils import set_user_joined, create_buy_order


def test_creating_new_user_should_update_firebase(
    mocker,
    request,
    firestore_client,
    user,
):
    if not user.is_joined:
        set_user_joined(mocker, user)

    time.sleep(30)

    doc_ref = firestore_client.collection("dev_portfolio").document(
        str(user.id),
    )

    doc = doc_ref.get()

    assert doc.exists

    doc_dict = doc.to_dict()

    print("Firebase doc: " + str(doc_dict))

    assert doc_dict["profile"]["username"] == user.username
    assert doc_dict["profile"]["email"] == user.email


def inactive_test_order_should_be_added_to_firebase(
    mocker,
    request,
    firestore_client,
    user,
):
    # just in case if the user is still not joined the competition
    if not user.is_joined:
        set_user_joined(mocker, user)

    # we create a new order here
    create_buy_order(
        price=7.13,
        ticker="6837.HK",
        margin=2,
        user_id=user.id,
        bot_id="UNO_OTM_007692",
    )

    time.sleep(30)

    doc_ref = firestore_client.collection("dev_portfolio").document(
        str(user.id),
    )

    doc = doc_ref.get()

    assert doc.exists

    doc_dict = doc.to_dict()
    print(doc_dict["active_portfolio"])

    assert doc_dict["active_portfolio"]
