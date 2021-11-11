from typing import List

import requests
from config.celery import app
from core.universe.models import ExchangeMarket
from django.conf import settings
from django.utils import timezone
from firebase_admin import firestore
from general.slack import report_to_slack
from ingestion.firestore_migration import firebase_universe_update, firebase_user_update
from tests.utils.firebase_schema import (
    FIREBASE_PORTFOLIO_SCHEMA,
    FIREBASE_UNIVERSE_SCHEMA,
)


def check_api() -> dict:
    def is_up(url: str) -> bool:
        response = requests.head(url)
        return response.status_code == 200

    droid_url: str = "https://dev-services.asklora.ai"
    droid_prod_url: str = "https://services.asklora.ai"

    return {
        "droid": "up" if is_up(droid_prod_url) else "down",
        "droid_dev": "up" if is_up(droid_url) else "down",
    }


def check_updater_schema() -> dict:
    invalid_portfolios: List[str] = []
    portfolios_df = firebase_user_update(
        currency_code=["HKD", "USD"],
        update_firebase=False,
    )
    portfolios = portfolios_df.to_dict("records")

    for portfolio in portfolios:
        try:
            FIREBASE_PORTFOLIO_SCHEMA.validate(portfolio)
        except:
            invalid_portfolios.append(portfolio["user_id"])

    invalid_tickers: List[str] = []
    universe_df = firebase_universe_update(
        currency_code=["HKD", "USD"],
        update_firebase=False,
    )

    universe = universe_df.to_dict("records")

    for ticker in universe:
        try:
            FIREBASE_UNIVERSE_SCHEMA.validate(ticker)
        except:
            invalid_tickers.append(ticker["ticker"])

    return {
        "portfolio_num": len(portfolios),
        "universe_num": len(universe),
        "portfolio_updater": "functional" if not invalid_portfolios else "error",
        "universe_updater": "functional" if not invalid_tickers else "error",
        "invalid_portfolios": invalid_portfolios,
        "invalid_tickers": invalid_tickers,
    }


def check_firebase_schema() -> dict:
    # staging_app = getattr(settings, "FIREBASE_STAGGING_APP", None)
    portfolio_collection = settings.FIREBASE_COLLECTION["portfolio"]
    universe_collection = settings.FIREBASE_COLLECTION["universe"]

    def check_portfolio_schema(portfolios_collection) -> dict:
        portfolio_fails: List[str] = []
        portfolios_num: int = 0

        for portfolio in portfolios_collection:
            portfolios_num += 1
            try:
                FIREBASE_PORTFOLIO_SCHEMA.validate(portfolio.to_dict())
            except:
                portfolio_fails.append(portfolio.to_dict()["user_id"])

        return {
            "portfolio_fails": portfolio_fails,
            "portfolio_num": portfolios_num,
        }

    def check_universe_schema(universe_collection) -> dict:
        universe_fails: List[str] = []
        universe_num: int = 0

        for ticker in universe_collection:
            universe_num += 1
            try:
                FIREBASE_UNIVERSE_SCHEMA.validate(ticker.to_dict())
            except:
                universe_fails.append(ticker.to_dict()["ticker"])

        return {
            "universe_fails": universe_fails,
            "universe_num": universe_num,
        }

    portfolio_check: dict = check_portfolio_schema(
        firestore.client().collection(portfolio_collection).stream()
    )
    universe_check: dict = check_universe_schema(
        firestore.client().collection(universe_collection).stream()
    )

    portfolio_fails, portfolio_num = portfolio_check.values()
    universe_fails, universe_num = universe_check.values()

    return {
        "portfolio_num": portfolio_num,
        "universe_num": universe_num,
        "portfolio_updater": "functional" if not portfolio_fails else "error",
        "universe_updater": "functional" if not universe_fails else "error",
        "invalid_portfolios": portfolio_fails,
        "invalid_tickers": universe_fails,
    }


def check_market() -> dict:
    def check_market_status() -> dict:
        status: dict = {}
        markets: List[str] = ["US.NASDAQ", "HK.HKEX"]
        token: str = "1M1a35Qhk8gUbCsOSl6XRY2z3Qjj0of7y5ZEfE5MasUYm5b9YsoooA7RSxW7"
        url: str = (
            "http://api.tradinghours.com/v3/markets/status?fin_id="
            + ",".join(markets)
            + "&token="
            + token
        )
        response = requests.get(url)
        if response.status_code == 200:
            response_body = response.json()
            for market in markets:
                status[market] = response_body["data"][market]["status"].lower()

        return status

    status = check_market_status()

    return {
        "usd_market_api": status["US.NASDAQ"],
        "hkd_market_api": status["HK.HKEX"],
        "usd_market_db": "open"
        if ExchangeMarket.objects.get(currency_code="USD").is_open
        else "closed",
        "hkd_market_db": "open"
        if ExchangeMarket.objects.get(currency_code="HKD").is_open
        else "closed",
    }


def send_email(data: dict) -> None:
    report_to_slack(data, channel="#droid_v2_test_report")


@app.task(ignore_result=True)
def daily_health_check():
    asklora_report: dict = {}
    droid_report: dict = {
        "api_status": check_api(),
        # "updater_schema": check_updater_schema(),
        "firebase_schema": check_firebase_schema(),
        "market_status": check_market(),
    }

    send_email(
        {
            "date": str(timezone.now()),
            "asklora_report": asklora_report,
            "droid_report": droid_report,
        }
    )
