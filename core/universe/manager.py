from django.db import models
from ingestion.universe import populate_universe_consolidated_by_isin_sedol_from_dsws
from general.sql_process import do_function


class ConsolidatedManager(models.Manager):

    def get_isin_code(self, ticker=None):

        if ticker:
            try:
                populate_universe_consolidated_by_isin_sedol_from_dsws(ticker=ticker)
                do_function("universe_populate")
                return True
            except Exception as e:
                print(e)
                return False
