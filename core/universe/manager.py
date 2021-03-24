from django.db import models
from ingestion.universe import populate_universe_consolidated_by_isin_sedol_from_dsws
from django.db import connection


class ConsolidatedManager(models.Manager):

    def get_isin_code(self, ticker=None):

        if ticker:
            try:
                populate_universe_consolidated_by_isin_sedol_from_dsws(ticker=ticker)
                
                return True
            except Exception as e:
                print(e)
                return False

class UniverseManager(models.Manager):
    
    def get_ticker_list_by_currency(self, currency=None):
        if currency:
            if isinstance(currency,list):
                currency = tuple(currency)
            table_name = connection.ops.quote_name(self.model._meta.db_table)
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT ticker FROM {table_name} WHERE is_active=True and currency_code in {currency}")
                row = cursor.fetchall()
            result = [_[0] for _ in row]
            return result
        else:
            return []
