from django.core.management.base import BaseCommand
import requests
import json

class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument("-b", "--branch", type=str, help="branch")

    def handle(self, *args, **options):
        if options.get("branch",None):
            head = options["branch"]
        else:
            head = "dev"

        data = json.dumps(
            {"ref":f"refs/heads/{head}"})
        print(data)
        workflow = requests.post('https://api.github.com/repos/asklora/DROID_V2.1/actions/workflows/13294075/dispatches',data=data,headers={
            "Authorization": "token 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",
            "Accept": "application/vnd.github.v3+json"
        })
        print(workflow.status_code)