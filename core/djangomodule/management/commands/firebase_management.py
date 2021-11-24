from core.user.models import User
from django.conf import settings
from django.core.management.base import BaseCommand
from firebase_admin import firestore
from schema import SchemaError
from tests.utils.firebase_schema import FIREBASE_PORTFOLIO_SCHEMA
from tests.utils.print import print_divider


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "-s",
            "--staging",
            action="store_true",
            default=False,
            help="Use staging scope",
        )
        parser.add_argument(
            "-l",
            "--list",
            action="store_true",
            default=False,
            help="List test users",
        )
        parser.add_argument(
            "-c",
            "--check",
            nargs="+",
            type=int,
            help="Check user data",
        )
        parser.add_argument(
            "-d",
            "--delete",
            nargs="+",
            type=int,
            help="Delete by document id",
        )

    def handle(self, *args, **options):
        staging = options["staging"]
        list = options["list"]
        check = options["check"]
        delete = options["delete"]

        staging_app = getattr(settings, "FIREBASE_STAGGING_APP", None)
        collection_name = settings.FIREBASE_COLLECTION["portfolio"]

        db = firestore.client()

        if staging:
            if staging_app:
                db = firestore.client(app=staging_app)
            else:
                self.stdout.write(self.style.NOTICE(f"Staging scope is not available"))

        if list:
            test_emails = [
                "ms-7979-652599d3@tests.com",
                "hp-14-notebook-pc-2c826ed5@tests.com",
                "hexboi@tests.com",
            ]

            query = db.collection(collection_name).where(
                "profile.email", "in", test_emails
            )
            result = query.get()

            self.stdout.write(
                self.style.NOTICE(f"There are {len(result)} test account(s)")
            )

            for doc in result:
                print(f"{doc.id} => {doc.to_dict()['profile']['email']}")

        elif check:
            query = db.collection(collection_name).where("user_id", "in", check)
            documents = query.get()
            for document in documents:
                portfolio = document.to_dict()
                try:
                    FIREBASE_PORTFOLIO_SCHEMA.validate(portfolio)
                except SchemaError as e:
                    print(e)

        elif delete:
            for id in delete:
                query = db.collection(collection_name).document(id)
                document = query.get()

                if document.exists:
                    self.stdout.write(self.style.WARNING(f"Deleting user {id}"))
                    query.delete()
                    self.stdout.write(self.style.SUCCESS("User deleted"))
                else:
                    self.stdout.write(self.style.NOTICE(f"User {id} not found"))

        else:
            print(
                "use --staging to work in the staging scope "
                f"(staging scope is {'available' if staging_app else 'not available'})\n"
                "use --list to list test accounts\n"
                "use --check with an user ID to check the user data\n"
                "use --delete with an user ID to delete the user's data\n"
            )
