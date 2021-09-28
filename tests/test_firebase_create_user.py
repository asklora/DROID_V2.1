def test_create_new_user_should_update_firebase(
    request,
    firestore_client,
    user,
):
    doc_ref = firestore_client.collection("dev_portfolio").document(
        str(user.id),
    )

    doc = doc_ref.get()

    assert doc.exists
