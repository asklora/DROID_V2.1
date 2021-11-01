import time

from bot.calculate_bot import populate_daily_profit
from django.conf import settings
from ingestion.firestore_migration import firebase_user_update
from tests.utils.firebase_schema import FIREBASE_SCHEMA
from tests.utils.order import (
    confirm_order,
    create_buy_order,
    create_sell_order,
)
from tests.utils.user import set_user_joined


def test_user_values(mocker, firestore_client, user, tickers):
    def calculate_pct(current_assets: float):
        # The asset from previous month is 200000 because
        # it's the first month for the test user
        # for real accounts, this will be 100000 HKD
        previous_assets: float = 200000.0

        print(f"current assets: {current_assets}\nprevious assets: {previous_assets}")

        percentage: float = (current_assets - previous_assets) / previous_assets * 100
        return percentage

    # just in case if the user is still not joined the competition
    if not user.is_joined:
        set_user_joined(mocker, user)

    # get random ticker
    ticker, price = tickers[4].values()

    # we create a new order here
    order = create_buy_order(
        price=price,
        ticker=ticker,
        margin=2,
        user_id=user.id,
        bot_id="UNO_OTM_007692",
    )

    # we confirm the order
    confirm_order(order)

    # then we sell it, after waiting fo a while
    sell_position, sell_order = create_sell_order(order)
    confirm_order(sell_order)

    # update firebases
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

    # assert the daily profit percentage
    assert doc_dict["daily_profit_pct"] == round(calculate_pct(doc_dict["current_asset"]), 2)

    # TODO: assert mtd

    # TODO: assert pnl
