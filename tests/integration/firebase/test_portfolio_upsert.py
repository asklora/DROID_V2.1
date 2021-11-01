import time

from bot.calculate_bot import populate_daily_profit
from django.conf import settings
from ingestion.firestore_migration import firebase_user_update
from tests.utils.firebase_schema import FIREBASE_SCHEMA
from tests.utils.order import confirm_order, create_buy_order
from tests.utils.user import set_user_joined


def test_order_should_be_updated_to_firebase(
    mocker,
    firestore_client,
    user,
    tickers,
):
    # just in case if the user is still not joined the competition
    if not user.is_joined:
        set_user_joined(mocker, user)

    # get random ticker
    ticker, price = tickers[2].values()

    # we create a new order here
    order = create_buy_order(
        price=price,
        ticker=ticker,
        margin=2,
        user_id=user.id,
        bot_id="UNO_OTM_007692",
    )

    confirm_order(order)

    # update firebase
    populate_daily_profit()
    firebase_user_update([user.id])

    # it takes a while to propagate to firebase so give it a second
    time.sleep(90)

    doc_ref = firestore_client.collection(
        settings.FIREBASE_COLLECTION["portfolio"],
    ).document(
        str(user.id),
    )

    doc = doc_ref.get()

    assert doc.exists

    doc_dict = doc.to_dict()
    print(doc_dict)

    # whether the data in the firebase is structured correctly
    assert FIREBASE_SCHEMA.validate(doc_dict)

    active_portfolios = doc_dict["active_portfolio"]

    # whether the order is put into firebase
    assert active_portfolios
    assert len(active_portfolios) == 1

    # whether the order data is correct
    assert (
        active_portfolios[len(active_portfolios) - 1]["order_uid"].replace(
            "-",
            "",
        )
        == order.order_uid
    )
    assert active_portfolios[len(active_portfolios) - 1]["share_num"] == order.qty

    # time.sleep(600)

    # whether the user's rank changes
    assert doc_dict["rank"] is not None
