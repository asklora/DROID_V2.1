from typing import List
from schema import SchemaError
from django.conf import settings
from django.core.management.base import BaseCommand
from firebase_admin import firestore
from ingestion.firestore_migration import firebase_user_update, firebase_universe_update
from tests.utils.firebase_schema import (
    FIREBASE_PORTFOLIO_SCHEMA,
    FIREBASE_RANKING_SCHEMA,
    FIREBASE_UNIVERSE_SCHEMA,
    FIREBASE_UNIVERSE_SCHEMA_DEVELOPMENT,
)
from tests.utils.print import print_divider


class Command(BaseCommand):
    def handle(self, *args, **options):
        """
        Settings and parametres
        """
        staging_app = getattr(settings, "FIREBASE_STAGGING_APP", None)
        portfolio_collection = settings.FIREBASE_COLLECTION["portfolio"]
        ranking_collection = settings.FIREBASE_COLLECTION["ranking"]
        universe_collection = settings.FIREBASE_COLLECTION["universe"]

        """
        Portfolio: upsert command assertion
        """
        print_divider("Check portfolio upsert command")
        result = firebase_user_update(
            currency_code=["HKD"],
            update_firebase=False,
        )

        assert type(result) is not str

        records = result.to_dict("records")

        for portfolio in records:
            assert FIREBASE_PORTFOLIO_SCHEMA.validate(portfolio)

        self.stdout.write(
            self.style.SUCCESS(
                f"Portfolio upsert command has the correct schema, {len(records)} checked",
            )
        )

        """
        Universe: upsert command assertion
        """
        # print_divider("Check universe upsert command")
        # result = firebase_universe_update(
        #     currency_code=["HKD"],
        #     update_firebase=False,
        # )

        # assert type(result) is not str

        # records = result.to_dict("records")

        # for portfolio in records:
        #     assert FIREBASE_PORTFOLIO_SCHEMA.validate(portfolio)

        # self.stdout.write(
        #     self.style.SUCCESS(
        #         f"HKD universe upsert command has the correct schema, {len(records)} checked",
        #     )
        # )

        # result = firebase_universe_update(
        #     currency_code=["USD"],
        #     update_firebase=False,
        # )

        # assert type(result) is not str

        # records = result.to_dict("records")

        # for portfolio in records:
        #     assert FIREBASE_PORTFOLIO_SCHEMA.validate(portfolio)

        # self.stdout.write(
        #     self.style.SUCCESS(
        #         f"USD universe upsert command has the correct schema, {len(records)} checked",
        #     )
        # )

        """
        Portfolio: firebase data assertion
        """
        print_divider("Check portfolio data on Firebase")
        portfolios = firestore.client().collection(portfolio_collection).stream()

        portfolio_fails: List[str] = []
        portfolios_num: int = 0

        for portfolio in portfolios:
            portfolios_num += 1
            try:
                FIREBASE_PORTFOLIO_SCHEMA.validate(portfolio.to_dict())
            except:
                portfolio_fails.append(portfolio.to_dict()["user_id"])

        if portfolio_fails:
            self.stdout.write(
                self.style.WARNING(
                    f"Portfolio data has data with incorrect schemas for: {', '.join(portfolio_fails)}\n({len(portfolio_fails)} of {portfolios_num})",
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Portfolio data on Firestore has the correct schema, {portfolios_num} checked",
                )
            )

        if staging_app:
            portfolios = (
                firestore.client(app=staging_app)
                .collection(portfolio_collection)
                .stream()
            )

            staging_portfolio_fails: List[str] = []
            staging_portfolios_num: int = 0

            for portfolio in portfolios:
                staging_portfolios_num += 1
                try:
                    FIREBASE_PORTFOLIO_SCHEMA.validate(portfolio.to_dict())
                except:
                    staging_portfolio_fails.append(portfolio.to_dict()["user_id"])

            if staging_portfolio_fails:
                self.stdout.write(
                    self.style.WARNING(
                        f"Portfolio data on staging has data with incorrect schemas for: {', '.join(staging_portfolio_fails)}\n({len(staging_portfolio_fails)} of {staging_portfolios_num})",
                    )
                )
            else:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Portfolio data on staging Firestore has the correct schema, {staging_portfolios_num} checked",
                    )
                )

        """
        Ranking: firebase data assertion
        """
        print_divider("Check ranking data on Firebase")
        rankings = firestore.client().collection(ranking_collection).stream()

        ranking_fails: List[str] = []
        rankings_num: int = 0
        for ranking in rankings:
            rankings_num += 1
            try:
                FIREBASE_RANKING_SCHEMA.validate(ranking.to_dict())
            except:
                ranking_fails.append(ranking.to_dict()["email"])

        if ranking_fails:
            self.stdout.write(
                self.style.WARNING(
                    f"Ranking data has data with incorrect schemas for: {', '.join(ranking_fails)}\n({len(ranking_fails)} of {rankings_num})",
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Ranking data on Firestore has the correct schema, {rankings_num} checked",
                )
            )

        if staging_app:
            rankings = (
                firestore.client(app=staging_app)
                .collection(ranking_collection)
                .stream()
            )

            staging_ranking_fails: List[str] = []
            staging_rankings_num: int = 0

            for ranking in rankings:
                staging_rankings_num += 1
                try:
                    FIREBASE_RANKING_SCHEMA.validate(ranking.to_dict())
                except:
                    staging_ranking_fails.append(ranking.to_dict()["email"])

            if staging_ranking_fails:
                self.stdout.write(
                    self.style.WARNING(
                        f"Ranking data on staging has data with incorrect schemas for: {', '.join(staging_ranking_fails)}\n({len(staging_ranking_fails)} of {staging_rankings_num})",
                    )
                )
            else:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Ranking data on staging Firestore has the correct schema, {staging_rankings_num} checked",
                    )
                )

        """
        Universe: firebase data assertion
        """
        print_divider("Check universe data on Firebase")
        tickers = firestore.client().collection(universe_collection).stream()

        tickers_fails: List[str] = []
        tickers_num: int = 0

        for ticker in tickers:
            tickers_num += 1
            try:
                FIREBASE_UNIVERSE_SCHEMA_DEVELOPMENT.validate(ticker.to_dict())
            except:
                tickers_fails.append(ticker.to_dict()["ticker"])

        if tickers_fails:
            self.stdout.write(
                self.style.WARNING(
                    f"Universe data has data with incorrect schemas: {', '.join(tickers_fails)}\n({len(tickers_fails)} of {tickers_num})",
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"Universe data on Firestore has the correct schema, {tickers_num} checked",
                )
            )

        if staging_app:
            tickers = (
                firestore.client(app=staging_app)
                .collection(universe_collection)
                .stream()
            )

            staging_tickers_fails: List[str] = []
            staging_tickers_num: int = 0

            for ticker in tickers:
                staging_tickers_num += 1
                try:
                    FIREBASE_UNIVERSE_SCHEMA_DEVELOPMENT.validate(ticker.to_dict())
                except:
                    staging_tickers_fails.append(ticker.to_dict()["ticker"])

            if staging_tickers_fails:
                self.stdout.write(
                    self.style.WARNING(
                        f"Universe data has data with incorrect schemas: {', '.join(staging_tickers_fails)}\n({len(staging_tickers_fails)} of {staging_tickers_num})",
                    )
                )
            else:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"Universe data on Firestore has the correct schema, {staging_tickers_num} checked",
                    )
                )

        print_divider()
