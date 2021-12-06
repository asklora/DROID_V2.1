from django.utils.translation import gettext


def test_translation(use_chinese) -> None:
    assert gettext("order not found") != "order not found"
    assert (
        gettext("order not found") == "Order cannot be found, but in Chinese"
    )
