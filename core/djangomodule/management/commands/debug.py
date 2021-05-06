from django.core.management.base import BaseCommand, CommandError
from core.user.models import User
from core.services.ingestiontask import migrate


class Command(BaseCommand):

    def handle(self, *args, **options):
        # user = User.objects.get(email="hkd_lm_adv@hanwha.asklora.ai")
        # print(user.client_user.all()[0].client.client_uid)
        migrate()
