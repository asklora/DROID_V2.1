from django.core.management.base import BaseCommand, CommandError
from core.user.models import User, Accountbalance, TransactionHistory
from core.Clients.models import UserClient, Client


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument("-e", "--email", type=str, help="email")
        parser.add_argument("-b", "--bot", type=str, help="bot group type")
        parser.add_argument("-p", "--password", type=str, help="ticker")
        parser.add_argument("-c", "--currency", type=str, help="currency")
        parser.add_argument("-amt", "--amt", type=float, help="amount")
        parser.add_argument("-cap", "--cap", type=str, help="capital")
        parser.add_argument("-type", "--type", type=str, help="capital")
        parser.add_argument("-s", "--service", type=str, help="capital")
        parser.add_argument(
            "-b2b", "--b2b", action="store_true", help="for client")
        parser.add_argument("-client", "--client",
                            type=str, help="client name")

        # parser.add_argument("-f4w", "--fels4w", action="store_true", help="only use test account")

    def handle(self, *args, **options):
        types = options["type"].upper()
        if types == "null" or types == "NULL":
            types = None
        try:
            user = User.objects.get(email=options["email"])
        except User.DoesNotExist:
            user = False

        if user:
            return print(f"email {user.email} is exist")

        if options["b2b"]:
            is_staff = True
        else:
            is_staff = False
        user = User.objects.create(
            email=options["email"],
            current_status="approved",
            is_active=True,
            is_staff=is_staff
        )
        user.set_password(options["password"])
        user.save()
        if user:
            wallet = Accountbalance.objects.create(
                user=user,
                currency_code_id=options["currency"].upper(),
                amount=options["amt"])
            try:
                client = Client.objects.get(
                    client_name=options["client"].upper())
            except Client.DoesNotExist:
                wallet.delete()
                user.delete()
                raise NameError("client not found")

            UserClient.objects.create(
                user=user,
                client=client,
                currency_code_id=options["currency"].upper(),
                extra_data={
                    "service_type":  options["service"],
                    "capital": options["cap"],
                    "type": types
                }
            )
