from schema import Optional, Or, Schema


FIREBASE_PORTFOLIO_SCHEMA = Schema(
    {
        "daily_profit_pct": Or(float, 0),
        "bot_pending_amount": Or(float, 0),
        "daily_invested_amount": Or(float, 0),
        "profile": {
            "birth_date": Or(str, None),
            "is_joined": bool,
            "phone": Or(str, None),
            "gender": Or(str, None),
            "first_name": Or(str, None),
            "username": str,
            "email": str,
            "last_name": Or(str, None),
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
                    "bot_details": {
                        "duration": str,
                        "bot_id": str,
                        "bot_option_type": str,
                        "bot_apps_name": str,
                        "bot_apps_description": Or(str, None),
                    },
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
                    "schi_name": Or(str, None),
                    "share_num": Or(float, 0),
                    "spot_date": str,
                    "status": str,
                    "stop_loss": Or(float, 0),
                    "take_profit": Or(float, 0),
                    "tchi_name": Or(str, None),
                    "threshold": Or(float, 0),
                    "ticker_fullname": Or(str, None),
                    "ticker_name": str,
                    "ticker": str,
                    "trading_day": str,
                    "user_id": int,
                }
            ],
            [],
        ),
        "total_profit_pct": Or(float, 0),
        "is_decimal": bool,
        "rank": Or(float, int, None),
        "daily_live_profit": Or(float, 0),
        "total_user_invested_amount": Or(float, 0),
    }
)

FIREBASE_RANKING_SCHEMA = Schema(
    {
        "email": str,
        "first_name": Or(str, None),
        "last_name": Or(str, None),
        "rank": Or(int, float),
        "ranking": str,
        "total_profit_pct": float,
        "user_id": int,
        "username": str,
    }
)

FIREBASE_UNIVERSE_SCHEMA = Schema(
    {
        "detail": {
            "company_description": str,
            "country": str,
            "currency_code": str,
            "industry_code": Or(str, None),
            "industry_group_code": Or(str, None),
            "industry_group_name": Or(str, None),
            "industry_name": Or(str, None),
            "lot_size": Or(float, int),
            "mic": str,
            "schi_name": Or(str, None),
            "tchi_name": Or(str, None),
            "ticker_fullname": str,
            "ticker_name": Or(str, None),
            "ticker_symbol": str,
        },
        "price": {
            "capital_change": Or(float, int),
            "close": Or(float, int),
            "ebitda": Or(float, int),
            "free_cash_flow": Or(float, int),
            "high": Or(float, int),
            "intraday_ask": Or(float, int),
            "intraday_bid": Or(float, int),
            "intraday_date": Or(str, None),
            "intraday_time": Or(str, None),
            "last_date": str,
            "latest_price": Or(float, int),
            "latest_price_change": Or(float, int),
            "low": Or(float, int),
            "market_cap": Or(float, int),
            "open": Or(float, int),
            "pb": Or(float, int),
            "pe_forecast": Or(float, int),
            "pe_ratio": Or(float, int),
            "revenue_per_share": Or(float, int),
            "volume": Or(float, int),
            "wk52_high": Or(float, int),
            "wk52_low": Or(float, int),
            Optional("dividen_yield"): Or(float, int),
            Optional("latest_net_change"): Or(float, int),
        },
        "ranking": Or(
            [
                {
                    "bot_apps_description": Or(str, None),
                    "bot_apps_name": str,
                    "bot_id": str,
                    "bot_return": Or(float, int),
                    "duration": str,
                    "risk_moderation": Or(float, int),
                    "win_rate": Or(float, int),
                }
            ],
            [],
        ),
        "rating": {
            "ai_score": Or(float, int),
            "ai_score2": Or(float, int),
            "dlp_1m": Or(float, int),
            "dlp_3m": Or(float, int),
            "esg": Or(float, int),
            "final_score": Or(float, int),
            "fundamentals_extra": Or(float, int),
            "fundamentals_quality": Or(float, int),
            "fundamentals_value": Or(float, int),
            "negative_factor": Or([str], []),
            "positive_factor": Or([str], []),
            "wts_rating": Or(float, int),
            "wts_rating2": Or(float, int),
        },
        "ticker": str,
    }
)

FIREBASE_UNIVERSE_SCHEMA_DEVELOPMENT = Schema(
    {
        "detail": {
            "company_description": str,
            "country": str,
            "currency_code": str,
            "industry_code": Or(str, None),
            "industry_group_code": Or(str, None),
            "industry_group_name": Or(str, None),
            "industry_name": Or(str, None),
            "lot_size": Or(float, int),
            "mic": str,
            "schi_name": Or(str, None),
            "tchi_name": Or(str, None),
            "ticker_fullname": str,
            "ticker_name": Or(str, None),
            "ticker_symbol": str,
        },
        "price": {
            "capital_change": Or(float, int),
            "close": Or(float, int),
            "ebitda": Or(float, int),
            "free_cash_flow": Or(float, int),
            "high": Or(float, int),
            "intraday_ask": Or(float, int),
            "intraday_bid": Or(float, int),
            "intraday_date": Or(str, None),
            "intraday_time": Or(str, None),
            "last_date": str,
            "latest_price": Or(float, int),
            "latest_price_change": Or(float, int),
            "low": Or(float, int),
            "market_cap": Or(float, int),
            "open": Or(float, int),
            "pb": Or(float, int),
            "pe_forecast": Or(float, int),
            "pe_ratio": Or(float, int),
            "revenue_per_share": Or(float, int),
            "volume": Or(float, int),
            "wk52_high": Or(float, int),
            "wk52_low": Or(float, int),
            "dividen_yield": Or(float, int),
            "latest_net_change": Or(float, int),
        },
        "ranking": Or(
            [
                {
                    "bot_apps_description": Or(str, None),
                    "bot_apps_name": str,
                    "bot_id": str,
                    "bot_return": Or(float, int),
                    "duration": str,
                    "risk_moderation": Or(float, int),
                    "win_rate": Or(float, int),
                }
            ],
            [],
        ),
        "rating": {
            "ai_score": Or(float, int),
            "ai_score2": Or(float, int),
            "dlp_1m": Or(float, int),
            "dlp_3m": Or(float, int),
            "esg": Or(float, int),
            "final_score": Or(float, int),
            "fundamentals_extra": Or(float, int),
            "fundamentals_quality": Or(float, int),
            "fundamentals_value": Or(float, int),
            "negative_factor": Or([str], []),
            "positive_factor": Or([str], []),
            "wts_rating": Or(float, int),
            "wts_rating2": Or(float, int),
        },
        "ticker": str,
    }
)
