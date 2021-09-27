import pytest

from general.sql_query import get_active_universe

pytestmark = pytest.mark.django_db(
    databases=[
        "default",
        "aurora_read",
        "aurora_write",
    ]
)


def test_should_return_active_universe():
    active_universe = get_active_universe()
    print("-" * 20)
    print(f"Active universe: {active_universe}")
    print("-" * 20)
