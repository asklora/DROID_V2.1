import logging
import time
from datetime import date, datetime
from typing import List

import requests
from celery.result import AsyncResult
from config.celery import app
from core.services.order_services import pending_order_checker
from core.universe.models import ExchangeMarket
from django.conf import settings
from django.utils import timezone
from firebase_admin import firestore
from general.slack import report_to_slack
from ingestion.firestore_migration import firebase_universe_update, firebase_user_update
from schema import SchemaError
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
        except SchemaError as e:
            portfolio_data = portfolio.to_dict()
            error: dict = {
                "user_id": portfolio_data["user_id"],
                "error": e,
            }
            invalid_portfolios.append(str(portfolio_data["user_id"]))

    invalid_tickers: List[str] = []
    universe_df = firebase_universe_update(
        currency_code=["HKD", "USD"],
        update_firebase=False,
    )

    universe = universe_df.to_dict("records")

    for ticker in universe:
        try:
            FIREBASE_UNIVERSE_SCHEMA.validate(ticker)
        except SchemaError as e:
            ticker_data = ticker.to_dict()
            error: dict = {
                "ticker": ticker_data["ticker"],
                "error": e,
            }
            invalid_tickers.append(str(portfolio_data["user_id"]))

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
        invalid_portfolios: List[str] = []
        portfolios_num: int = 0

        for portfolio in portfolios_collection:
            portfolios_num += 1
            try:
                FIREBASE_PORTFOLIO_SCHEMA.validate(portfolio.to_dict())
            except SchemaError as e:
                portfolio_data = portfolio.to_dict()
                error: dict = {
                    "user_id": portfolio_data["user_id"],
                    "error": e,
                }
                logging.warning("Porfolio scheme mismatch")
                print(error)
                invalid_portfolios.append(str(portfolio_data["user_id"]))

        return {
            "invalid_portfolios": invalid_portfolios,
            "portfolio_num": portfolios_num,
        }

    def check_universe_schema(universe_collection) -> dict:
        invalid_tickers: List[str] = []
        universe_num: int = 0

        for ticker in universe_collection:
            universe_num += 1
            try:
                FIREBASE_UNIVERSE_SCHEMA.validate(ticker.to_dict())
            except SchemaError as e:
                ticker_data = ticker.to_dict()
                error: dict = {
                    "ticker": ticker_data["ticker"],
                    "error": e,
                }
                logging.warning("Universe scheme mismatch")
                print(error)
                invalid_tickers.append(str(ticker_data["ticker"]))

        return {
            "invalid_tickers": invalid_tickers,
            "universe_num": universe_num,
        }

    portfolio_check: dict = check_portfolio_schema(
        firestore.client().collection(portfolio_collection).stream()
    )
    universe_check: dict = check_universe_schema(
        firestore.client().collection(universe_collection).stream()
    )

    invalid_portfolios, portfolio_num = portfolio_check.values()
    invalid_tickers, universe_num = universe_check.values()

    if staging_app:
        staging_portfolio_check: dict = check_portfolio_schema(
            firestore.client(app=staging_app).collection(portfolio_collection).stream()
        )
        staging_universe_check: dict = check_universe_schema(
            firestore.client(app=staging_app).collection(universe_collection).stream()
        )

        (
            staging_invalid_portfolios,
            staging_portfolio_num,
        ) = staging_portfolio_check.values()
        staging_universe_fails, staging_universe_num = staging_universe_check.values()

    return {
        "portfolio_num": portfolio_num,
        "universe_num": universe_num,
        "invalid_portfolios": invalid_portfolios,
        "invalid_tickers": invalid_tickers,
        "staging_portfolio_num": staging_portfolio_num if staging_app else [],
        "staging_universe_num": staging_universe_num if staging_app else [],
        "staging_invalid_portfolios": staging_invalid_portfolios if staging_app else [],
        "staging_invalid_tickers": staging_universe_fails if staging_app else [],
    }


def check_market() -> dict:
    def force_open_market(market: dict) -> dict:
        try:
            market: ExchangeMarket = ExchangeMarket.objects.get(
                fin_id=market.get("fin_id")
            )
            market.is_open = True
            market.save()

            pending_order_checker.apply(args=(market.get("currency")))
        except:
            print("Market not found")

    status: dict = {}
    us_market: dict = {"currency": "USD", "fin_id": "US.NASDAQ"}
    hk_market: dict = {"currency": "HKD", "fin_id": "HK.HKEX"}

    # get data from TradingHours
    markets: List[str] = [market.get("fin_id") for market in [us_market, hk_market]]
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

    us_market_status_api: str = status.get(us_market["fin_id"])
    hk_market_status_api: str = status.get(hk_market["fin_id"])

    us_market_status_db: str = (
        "open"
        if ExchangeMarket.objects.get(fin_id=us_market["fin_id"]).is_open
        else "closed"
    )
    hk_market_status_db: str = (
        "open"
        if ExchangeMarket.objects.get(fin_id=hk_market["fin_id"]).is_open
        else "closed"
    )

    # if either of the market status from API doesn't match
    # the data in the db, force open it if it's closed
    if us_market_status_api == "open" and us_market_status_db != us_market_status_api:
        force_open_market(market=us_market)

    if hk_market_status_api == "open" and hk_market_status_db != hk_market_status_api:
        force_open_market(market=hk_market)

    return {
        "us_market_api": us_market_status_api,
        "hk_market_api": hk_market_status_api,
        "us_market_db": us_market_status_db,
        "hk_market_db": hk_market_status_db,
    }


def check_testproject() -> dict:
    status: dict = {}
    api_key: str = "okSRWQSYYFAr7LZvVkczgvyEpm5h1TkYWvSAEm-GAz41"
    project_id: str = "GGUk2NYP4k2YZVYIVSVlUg"
    job_id: str = "YaTSnhGMxESupCgfkNO08g"
    url: str = (
        "https://api.testproject.io/v2/projects/"
        + project_id
        + "/jobs/"
        + job_id
        + "/reports/latest?format=TestProject"
    )
    response = requests.get(url, headers={"Authorization": api_key})
    if response.status_code == 200:
        response_body = response.json()

        latest_test_date: datetime = datetime.strptime(
            response_body["executionEnd"].split(".")[0],
            "%Y-%m-%dT%H:%M:%S",
        )

        if latest_test_date.date() == date.today():
            status["date"] = latest_test_date
            status["status"] = response_body["testResults"][0]["result"]

    return status


def check_asklora() -> dict:
    # payload = {
    #     "type": "function",
    #     "module": "tests.healthcheck.check_asklora",
    #     "payload": {"date": date.today()},
    # }
    payload = {
        "type": "function",
        "module": "core.djangomodule.crudlib.user.get_user",
        "payload": {"username": "agustian"},
    }
    task = app.send_task(
        "config.celery.listener",
        args=(payload,),
        queue=settings.ASKLORA_QUEUE,
    )
    celery_result = AsyncResult(task.id, app=app)

    while celery_result.state == "PENDING":
        time.sleep(2)

    return celery_result.result


def send_report(data: dict) -> None:
    date, asklora_report, droid_report = data.values()
    (
        api_status,
        firebase_schema,
        market_status,
        testproject_status,
    ) = droid_report.values()

    # reports from asklora
    print(asklora_report.get("celery_status"))
    asklora_report: str = (
        f"- Celery is {'working' if asklora_report.get('celery_status') else 'having a problem'}\n"
        if asklora_report
        else "- No reports yet.\n"
    )

    # reports from droid
    schema_has_issues: bool = (
        len(firebase_schema["invalid_portfolios"]) > 0
        or len(firebase_schema["invalid_tickers"]) > 0
        or len(firebase_schema["staging_invalid_portfolios"]) > 0
        or len(firebase_schema["staging_invalid_tickers"]) > 0
    )
    schema_issues: str = ""
    if schema_has_issues:
        if firebase_schema["invalid_portfolios"]:
            schema_issues += (
                "\nThese users' data have inconsistent schema: "
                f"{', '.join(firebase_schema['invalid_portfolios'])}"
            )
        if firebase_schema["invalid_tickers"]:
            schema_issues += (
                "\nThese tickers' data have inconsistent schema: "
                f"{', '.join(firebase_schema['invalid_tickers'])}"
            )
        if firebase_schema["staging_invalid_portfolios"]:
            schema_issues += (
                "\nThese users' data have inconsistent schema in staging: "
                f"{', '.join(firebase_schema['staging_invalid_portfolios'])}"
            )
        if firebase_schema["staging_invalid_tickers"]:
            schema_issues += (
                "\nThese tickers' data have inconsistent schema in staging: "
                f"{', '.join(firebase_schema['staging_invalid_tickers'])}"
            )

    us_market_is_correct: bool = (
        market_status["us_market_api"] == market_status["us_market_db"]
    )
    hk_market_is_correct: bool = (
        market_status["hk_market_api"] == market_status["hk_market_db"]
    )
    us_market_difference: str = (
        "TradingHours API indicates that the market is "
        f"*{market_status['us_market_api']}* "
        f"but the market status in the db is *{market_status['us_market_db']}* "
        "(fixed)"
    )
    hk_market_difference: str = (
        "TradingHours API indicates that the market is "
        f"*{market_status['hk_market_api']}* "
        f"but the market status in the db is *{market_status['hk_market_db']}* "
        "(fixed)"
    )

    droid_report: str = (
        (
            f"- Droid API is *{api_status['droid']}*, Droid dev is *{api_status['droid_dev']}*\n"
            f"- Data in firebase *{'is correct' if not schema_has_issues else 'has schema mismatch'}*{schema_issues}\n"
            f"- US market status {'*is correct*' if us_market_is_correct else '*is incorrect* ,' + us_market_difference}\n"
            f"- HK market status {'*is correct*' if hk_market_is_correct else '*is incorrect* ,' + hk_market_difference}\n"
            f"- {'Latest TestProject test is *' + testproject_status['status'].lower() + '*' if testproject_status else '*No* TestProject test runs yet today'}\n"
        )
        if data["droid_report"]
        else "- No reports yet."
    )
    template = (
        f"Health check for *{date}*\n"
        f"{'=' * 25}\n"
        f"*Asklora report*:\n{asklora_report}"
        f"{'=' * 25}\n"
        f"*Droid report*:\n{droid_report}"
        f"{'=' * 25}\n"
    )
    report_to_slack(template, channel="#healthcheck")


@app.task(ignore_result=True)
def daily_health_check():
    asklora_report: dict = {"celery_status": check_asklora()}
    droid_report: dict = {
        "api_status": check_api(),
        # "updater_schema": check_updater_schema(),
        "firebase_schema": check_firebase_schema(),
        "market_status": check_market(),
        "testproject_status": check_testproject(),
    }

    send_report(
        {
            "date": timezone.now().strftime("%A, %d %B %Y"),
            "asklora_report": asklora_report,
            "droid_report": droid_report,
        }
    )
