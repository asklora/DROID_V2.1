import socket

from core.user.models import Accountbalance, User, UserDepositHistory
from django.core.management import call_command
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument(
            "--delete",
            action="store_true",
            dest="delete",
            default=False,
            help="Delete instead of closing it",
        )

    def handle(self, *args, **options):
        computer_name = socket.gethostname().lower()
        unique_email = f"{computer_name}@tests.com"

        if options["delete"]:
            try:
                user = User.objects.get(email=unique_email)
            except User.DoesNotExist:
                return print("user does not exist")

            call_command("delete_user", username=user.username)

        else:
            try:
                user = User.objects.get(email=unique_email)
                return print(f"email {user.email} exists")
            except User.DoesNotExist:
                user = None

            user = User.objects.create(
                email=unique_email,
                username=computer_name,
                is_active=True,
                current_status="verified",
            )
            user.set_password("everything_is_but_a_test")
            user.save()

            if user:
                user_balance = Accountbalance.objects.create(
                    user=user,
                    amount=0,
                    currency_code_id="HKD",
                )

                user.is_joined = True
                user.save()

                deposit_history: list[
                    UserDepositHistory
                ] = UserDepositHistory.objects.filter(user_id=user.id)

                assert deposit_history.exists() == True

                last_deposit = deposit_history.last()

                print(last_deposit.deposit if last_deposit else "No record")

                assert last_deposit.deposit == user.current_assets
