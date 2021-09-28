import time
from tests.utils import set_user_joined


def test_user_should_have_active_portfolios(
    mocker,
    request,
    firestore_client,
    user,
):
    if not user.is_joined:
        set_user_joined(mocker, user)

    time.sleep(3)

    doc_ref = firestore_client.collection("dev_portfolio").document(
        str(user.id),
    )

    doc = doc_ref.get()

    assert doc.exists

    portfolio = doc.to_dict()

    print(doc)
    print(portfolio)

    # active_portfolios = user_doc["active_portfolio"]

    # print(active_portfolios)

    # assert len(active_portfolios) > 0
