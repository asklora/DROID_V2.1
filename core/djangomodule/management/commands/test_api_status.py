import requests
from django.core.management.base import BaseCommand
from tests.utils.print import print_divider


def check_code(url: str) -> int:
    response = requests.head(url)
    return response.status_code


class Command(BaseCommand):
    def handle(self, *args, **options):
        """
        Settings and parametres
        """
        asklora_url: str = "https://api-dev.asklora.ai"
        asklora_prod_url: str = "https://api.asklora.ai"
        droid_url: str = "https://dev-services.asklora.ai"
        droid_prod_url: str = "https://services.asklora.ai"

        """
        Ping asklora database
        """
        print_divider("Pinging AskLora API backed")
        if check_code(asklora_url) != 200:
            self.stdout.write(self.style.ERROR("asklora API is down"))
        else:
            self.stdout.write(self.style.SUCCESS("asklora API is up and running"))

        if check_code(asklora_prod_url) != 200:
            self.stdout.write(self.style.ERROR("asklora dev API is down"))
        else:
            self.stdout.write(self.style.SUCCESS("asklora dev API is up and running"))

        print_divider("Pinging Droid API backed")
        if check_code(droid_url) != 200:
            self.stdout.write(self.style.ERROR("droid API is down"))
        else:
            self.stdout.write(self.style.SUCCESS("droid API is up and running"))

        if check_code(droid_prod_url) != 200:
            self.stdout.write(self.style.ERROR("droid dev API is down"))
        else:
            self.stdout.write(self.style.SUCCESS("droid dev API is up and running"))

        print_divider()
