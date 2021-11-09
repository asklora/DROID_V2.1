from django.conf import settings
from django.core.management.base import BaseCommand
from firebase_admin import firestore
from ingestion.firestore_migration import firebase_user_update, firebase_universe_update
from tests.utils.firebase_schema import (
    FIREBASE_PORTFOLIO_SCHEMA,
    FIREBASE_RANKING_SCHEMA,
    FIREBASE_UNIVERSE_SCHEMA,
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

        for portfolio in portfolios:
            assert FIREBASE_PORTFOLIO_SCHEMA.validate(portfolio.to_dict())

        self.stdout.write(
            self.style.SUCCESS(
                f"Portfolio data on Firestore has the correct schema",
            )
        )

        if staging_app:
            portfolios = (
                firestore.client(app=staging_app)
                .collection(portfolio_collection)
                .stream()
            )

            for portfolio in portfolios:
                assert FIREBASE_PORTFOLIO_SCHEMA.validate(portfolio.to_dict())

            self.stdout.write(
                self.style.SUCCESS(
                    f"Portfolio data on Firestore on staging has the correct schema",
                )
            )

        """
        Ranking: firebase data assertion
        """
        print_divider("Check ranking data on Firebase")
        portfolios = firestore.client().collection(ranking_collection).stream()

        for portfolio in portfolios:
            assert FIREBASE_RANKING_SCHEMA.validate(portfolio.to_dict())

        self.stdout.write(
            self.style.SUCCESS(
                f"Ranking data on Firestore has the correct schema",
            )
        )

        if staging_app:
            portfolios = (
                firestore.client(app=staging_app)
                .collection(ranking_collection)
                .stream()
            )

            for portfolio in portfolios:
                assert FIREBASE_RANKING_SCHEMA.validate(portfolio.to_dict())

            self.stdout.write(
                self.style.SUCCESS(
                    f"Ranking data on Firestore on staging has the correct schema",
                )
            )

        """
        Universe: firebase data assertion
        """
        print_divider("Check universe data on Firebase")
        portfolios = firestore.client().collection(universe_collection).stream()

        for portfolio in portfolios:
            assert FIREBASE_UNIVERSE_SCHEMA.validate(portfolio.to_dict())

        self.stdout.write(
            self.style.SUCCESS(
                f"Universe data on Firestore has the correct schema",
            )
        )

        if staging_app:
            portfolios = (
                firestore.client(app=staging_app)
                .collection(universe_collection)
                .stream()
            )

            for portfolio in portfolios:
                assert FIREBASE_UNIVERSE_SCHEMA.validate(portfolio.to_dict())

            self.stdout.write(
                self.style.SUCCESS(
                    f"Universe data on Firestore on staging has the correct schema",
                )
            )

        print_divider()
