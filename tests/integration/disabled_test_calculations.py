from bot.calculate_bot import update_season


def mock_upsert_data_to_database(
    data,
    table,
    primary_key,
    how="update",
    cpu_count=False,
    Text=False,
    Date=False,
    Int=False,
    Bigint=False,
    Bool=False,
    debug=False,
):
    print("\n=== mocked ===")
    print(data)


def test_update_season(mocker) -> None:
    mocked = mocker.patch(
        "bot.calculate_bot.upsert_data_to_database",
        wraps=mock_upsert_data_to_database,
    )

    update_season()

    mocked.assert_called()
