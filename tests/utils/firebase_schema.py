from schema import Or, Schema


FIREBASE_SCHEMA = Schema(
    {
        "daily_profit_pct": Or(float, 0),
        "bot_pending_amount": Or(float, 0),
        "daily_invested_amount": Or(float, 0),
        "profile": {
            "birth_date": str,
            "is_joined": bool,
            "phone": str,
            "gender": str,
            "first_name": str,
            "username": str,
            "email": str,
            "last_name": str,
        },
        "total_profit_amount": Or(float, 0),
        "total_bot_invested_amount": Or(float, 0),
        "total_portfolio": Or(float, 0),
        "total_invested_amount": Or(float, 0),
        "stock_pending_amount": Or(float, 0),
        "pct_total_bot_invested_amount": Or(float, 0),
        "current_asset": Or(float, 0),
        "pending_amount": Or(float, 0),
        "currency": str,
        "total_profit": Or(float, 0),
        "daily_profit": Or(float, 0),
        "balance": Or(float, 0),
        "user_id": int,
        "pct_total_user_invested_amount": Or(float, 0),
        "active_portfolio": Or(
            [
                {
                    "barrier": Or(float, 0),
                    "bot_cash_balance": Or(float, 0),
                    "currency_code": str,
                    "current_ivt_amt": Or(float, 0),
                    "current_values": Or(float, 0),
                    "entry_price": Or(float, 0),
                    "exchange_rate": Or(float, 0),
                    "expiry": str,
                    "investment_amount": Or(float, 0),
                    "margin_amount": Or(float, 0),
                    "margin": int,
                    "order_uid": str,
                    "pct_cash": Or(float, 0),
                    "pct_profit": Or(float, 0),
                    "pct_stock": Or(float, 0),
                    "position_uid": str,
                    "price": Or(float, 0),
                    "profit": Or(float, 0),
                    "share_num": Or(float, 0),
                    "spot_date": str,
                    "status": str,
                    "stop_loss": Or(float, 0),
                    "take_profit": Or(float, 0),
                    "threshold": Or(float, 0),
                    "ticker_name": str,
                    "ticker": str,
                    "trading_day": str,
                    "user_id": int,
                    "bot_details": {
                        "duration": str,
                        "bot_id": str,
                        "bot_option_type": str,
                        "bot_apps_name": str,
                        "bot_apps_description": str,
                    },
                }
            ],
            [],
        ),
        "total_profit_pct": Or(float, 0),
        "is_decimal": bool,
        "rank": Or(float, None),
        "daily_live_profit": Or(float, 0),
        "total_user_invested_amount": Or(float, 0),
    }
)
