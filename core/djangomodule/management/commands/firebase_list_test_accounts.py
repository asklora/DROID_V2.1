from core.user.models import User
from django.conf import settings
from django.core.management.base import BaseCommand
from firebase_admin import firestore


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "-l",
            "--list",
            action="store_true",
            default=True,
            help="List all test accounts in firebase",
        )
        parser.add_argument(
            "-s",
            "--staging",
            action="store_true",
            default=False,
            help="Use Staging project scope",
        )
        parser.add_argument(
            "-d",
            "--delete",
            action="append",
            type=str,
            help="Delete by document id",
        )

    def handle(self, *args, **options):
        list = options["list"]
        staging = options["staging"]
        delete = options["delete"]

        test_emails = [
            "ms-7979-652599d3@tests.com",
            "hp-14-notebook-pc-2c826ed5@tests.com",
            "hexboi@tests.com",
        ]

        db = firestore.client()
        collection_name = settings.FIREBASE_COLLECTION["portfolio"]
        query = db.collection(collection_name).where("profile.email", "in", test_emails)

        if staging:
            firebase_app = getattr(settings, "FIREBASE_STAGGING_APP", None)
            if firebase_app:
                print("Using staging app project")
                db = firestore.client(app=firebase_app)
            else:
                print("No staging app available")

        if list:
            result = query.get()

            self.stdout.write(
                self.style.NOTICE(f"There are {len(result)} test account(s)")
            )

            for doc in result:
                print(f"{doc.id} => {doc.to_dict()['email']}")

        elif delete:
            query = db.collection(collection_name).document(delete)
            document = query.get()

            if document.exists:
                self.stdout.write(self.style.WARNING(f"Deleting user {delete}"))
                document.delete()
                self.stdout.write(self.style.SUCCESS("User deleted"))
            else:
                self.stdout.write(self.style.NOTICE(f"User {delete} not found"))

        else:
            self.stdout.write(self.style.NOTICE(f"Doing nothing..."))
