from django.core.management.base import BaseCommand, CommandError
from core.user.models import User
import uuid

class Command(BaseCommand):
    def create_unique_username(self, email):
        strip = email.replace('@', '_').replace(
            '.com', '').replace('.', '_')
        unique_usr = "%s%s" % (uuid.uuid4().hex[:8], strip)
        return unique_usr
    def handle(self, *args, **options):
       users = User.objects.all()
       for user in users:
           if not user.username:
               user.username = self.create_unique_username(user.email)
               print(user.username)
               user.save()
