from django.core.management.base import BaseCommand, CommandError
import pandas as pd
from datetime import datetime



class Command(BaseCommand):

    def handle(self, *args, **options):
        df = pd.read_json(open('data.json'))
        # df = df.loc[df['group'].notnull()]
        print( df[df.duplicated(['mic'])])
        # df.to_json("data.json",orient="records")
