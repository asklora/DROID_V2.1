from django.core.management.base import BaseCommand, CommandError
from core.services.upload_to_s3 import s3_uploader
class Command(BaseCommand):

    def handle(self, *args, **options):
    	text = "test2"
    	upload = s3_uploader(content=text, dir_name="test_dir",bucket_name=None, filename=None)
    	print(upload)