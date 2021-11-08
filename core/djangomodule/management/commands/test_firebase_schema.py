from django.conf import settings
from django.core.management.base import BaseCommand
from firebase_admin import firestore
from ingestion.firestore_migration import firebase_user_update
from tests.utils.firebase_schema import FIREBASE_SCHEMA
from tests.utils.print import print_divider


class Command(BaseCommand):
    def handle(self, *args, **options):
        """
        Settings and parametres
        """
        staging_app = getattr(settings, "FIREBASE_STAGGING_APP", None)
        portfolio_collection = settings.FIREBASE_COLLECTION["portfolio"]
        # universe_collection = settings.FIREBASE_COLLECTION["universe"]s

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
            assert FIREBASE_SCHEMA.validate(portfolio)

        self.stdout.write(
            self.style.SUCCESS(
                f"Portfolio upsert command has the correct schema, {len(records)} checked",
            )
        )

        """
        Portfolio: firebase data assertion
        """
        print_divider("Check portfolio data on Firebase")
        portfolios = firestore.client().collection(portfolio_collection).stream()

        for portfolio in portfolios:
            assert FIREBASE_SCHEMA.validate(portfolio.to_dict())

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
                assert FIREBASE_SCHEMA.validate(portfolio.to_dict())

            self.stdout.write(
                self.style.SUCCESS(
                    f"Portfolio data on Firestore on staging has the correct schema",
                )
            )

        print_divider()
