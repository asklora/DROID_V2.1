from typing import List
import pytest
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
    staging_app = getattr(settings, "FIREBASE_STAGGING_APP", None)
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
                portfolio_fails.append(str(portfolio.to_dict()["user_id"]))

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

    if staging_app:
        staging_portfolio_check: dict = check_portfolio_schema(
            firestore.client(app=staging_app).collection(portfolio_collection).stream()
        )
        staging_universe_check: dict = check_universe_schema(
            firestore.client(app=staging_app).collection(universe_collection).stream()
        )

        (
            staging_portfolio_fails,
            staging_portfolio_num,
        ) = staging_portfolio_check.values()
        staging_universe_fails, staging_universe_num = staging_universe_check.values()

    return {
        "portfolio_num": portfolio_num,
        "universe_num": universe_num,
        "invalid_portfolios": portfolio_fails,
        "invalid_tickers": universe_fails,
        "staging_portfolio_num": staging_portfolio_num if staging_app else [],
        "staging_universe_num": staging_universe_num if staging_app else [],
        "staging_invalid_portfolios": staging_portfolio_fails if staging_app else [],
        "staging_invalid_tickers": staging_universe_fails if staging_app else [],
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


def run_tests() -> dict:
    retcode = pytest.main(["tests/unit/test_currency_conversion.py"])
    if retcode == pytest.ExitCode.OK:
        return {"status": "success"}
    else:
        return {"status": "error"}


def send_email(data: dict) -> None:
    date, asklora_report, droid_report = data.values()
    api_status, firebase_schema, market_status, tests = droid_report.values()

    schema_has_issues: bool = (
        len(firebase_schema["invalid_portfolios"]) > 0
        or len(firebase_schema["invalid_tickers"]) > 0
        or len(firebase_schema["staging_invalid_portfolios"]) > 0
        or len(firebase_schema["staging_invalid_tickers"]) > 0
    )
    schema_issues: str = (
        f". These users have wrong schema: "
        f"{', '.join(firebase_schema['invalid_portfolios'])}\n"
        if firebase_schema["invalid_portfolios"]
        else ""
        f". These tickers have wrong schema: "
        f"{', '.join(firebase_schema['invalid_tickers'])}\n"
        if firebase_schema["invalid_tickers"]
        else ""
        f". These users have wrong schema in staging: "
        f"{', '.join(firebase_schema['staging_invalid_portfolios'])}\n"
        if firebase_schema["staging_invalid_portfolios"]
        else ""
        f". These tickers have wrong schema in staging: "
        f"{', '.join(firebase_schema['staging_invalid_tickers'])}\n"
        if firebase_schema["staging_invalid_tickers"]
        else "\n"
    )
    usd_market_is_correct: bool = (
        market_status["usd_market_api"] == market_status["usd_market_db"]
    )
    hkd_market_is_correct: bool = (
        market_status["hkd_market_api"] == market_status["hkd_market_db"]
    )

    asklora_report: str = (
        (f"- Api is {'up' if asklora_report['api_status']['asklora'] else 'down'}\n")
        if asklora_report
        else "- No reports yet.\n"
    )
    droid_report: str = (
        (
            f"- Droid API is {api_status['droid']}, Droid dev is {api_status['droid_dev']}\n"
            f"- Data in firebase {'is correct' if not schema_has_issues else 'has some issues'}{schema_issues}"
            f"- USD market status {'is correct' if usd_market_is_correct else 'is incorrect'}\n"
            f"- HKD market status {'is correct' if hkd_market_is_correct else 'is incorrect'}\n"
            f"- Tests {'are successful' if tests['status'] == 'success' else 'have some error'}\n"
        )
        if data["droid_report"]
        else "- No reports yet."
    )
    template = (
        f"Health check for {date}\n"
        f"Asklora report:\n{asklora_report}"
        f"Droid report:\n{droid_report}"
    )
    report_to_slack(template, channel="#healthcheck")


@app.task(ignore_result=True)
def daily_health_check():
    asklora_report: dict = {}
    droid_report: dict = {
        "api_status": check_api(),
        # "updater_schema": check_updater_schema(),
        "firebase_schema": check_firebase_schema(),
        "market_status": check_market(),
        "tests": run_tests(),
    }

    send_email(
        {
            "date": str(timezone.now()),
            "asklora_report": asklora_report,
            "droid_report": droid_report,
        }
    )
