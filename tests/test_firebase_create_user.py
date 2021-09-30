import time

from tests.utils import set_user_joined


def inactive_test_create_new_user_should_update_firebase(
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
