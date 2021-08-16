from django.core.management.base import BaseCommand
from core.user.models import User
class Command(BaseCommand):
    
    def handle(self, *args, **options):
        users  = User.objects.filter(current_status='verified')
        for user in users:
            user.delete()