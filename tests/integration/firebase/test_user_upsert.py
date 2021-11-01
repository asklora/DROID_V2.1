import time

from django.conf import settings
from tests.utils.firebase_schema import FIREBASE_SCHEMA
from tests.utils.user import set_user_joined


def test_creating_new_user_should_update_firebase(
    mocker,
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
    assert FIREBASE_SCHEMA.validate(doc_dict)

    print("Firebase doc: " + str(doc_dict))

    # check if the data matches
    assert doc_dict["profile"]["username"] == user.username
    assert doc_dict["profile"]["email"] == user.email