from django.core.management.base import BaseCommand
from general.date_process import dateNow
from general.sql_process import do_function
from general.sql_output import fill_null_quandl_symbol
from ingestion.master_multiple import master_multiple_update
from ingestion.master_tac import master_tac_update
from ingestion.master_ohlcvtr import master_ohlctr_update
from main import daily_ingestion, daily_process_ohlcvtr
from migrate import daily_migrations, weekly_migrations
from ingestion.currency import (
    update_currency_price_from_dss,
    update_utc_offset_from_timezone
)
from bot.preprocess import (
    dividend_daily_update, 
    interest_daily_update
)
from ingestion.ai_value import (
    populate_ibes_table,
    populate_macro_table,
    update_fred_data_from_fred,
    update_ibes_data_monthly_from_dsws,
    update_macro_data_monthly_from_dsws,
    update_worldscope_quarter_summary_from_dsws
)
from ingestion.master_data import (
    update_quandl_orats_from_quandl,
    update_fundamentals_score_from_dsws, 
    update_fundamentals_quality_value,
    update_vix_from_dsws,
    interest_update,
    dividend_updated
    )
from ingestion.universe import (
    update_ticker_name_from_dsws,
    update_entity_type_from_dsws,
    update_currency_code_from_dss,
    update_mic_from_dss,
    update_lot_size_from_dss,
    update_company_desc_from_dsws,
    update_worldscope_identifier_from_dsws,
    update_industry_from_dsws,
    update_ticker_symbol_from_dss
)


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("-na", "--na", type=bool, help="na", default=False)
        parser.add_argument("-ws", "--ws", type=bool, help="ws", default=False)
        parser.add_argument("-worldscope", "--worldscope", type=bool, help="worldscope", default=False)
        parser.add_argument("-fundamentals_score", "--fundamentals_score", type=bool, help="fundamentals_score", default=False)
        parser.add_argument("-quandl", "--quandl", type=bool, help="quandl", default=False)
        parser.add_argument("-vix", "--vix", type=bool, help="vix", default=False)
        parser.add_argument("-interest", "--interest", type=bool, help="interest", default=False)
        parser.add_argument("-dividend", "--dividend", type=bool, help="dividend", default=False)
        parser.add_argument("-utc_offset", "--utc_offset", type=bool, help="utc_offset", default=False)
        parser.add_argument("-weekly", "--weekly", type=bool, help="weekly", default=False)
        parser.add_argument("-monthly", "--monthly", type=bool, help="monthly", default=False)
        parser.add_argument("-currency_code", "--currency_code", nargs="+", help="currency_code", default=None)

    def handle(self, *args, **options):
        try:
            status = ""
            if (options["na"]):
                status = "Currency Price Update"
                update_currency_price_from_dss()
                status = "Daily Ingestion Update"
                daily_migrations()
                daily_ingestion(region_id="na")
                # daily_process_ohlcvtr(region_id = "na")
                do_function("special_cases_1")
                do_function("master_ohlcvtr_update")
                status = "Master OHLCVTR Update"
                master_ohlctr_update()
                status = "Master TAC Update"
                master_tac_update()
                status = "Master Multiple Update"
                master_multiple_update()
                interest_update()
                dividend_daily_update()
                interest_daily_update()
                status = "Macro Ibes Update"
                populate_macro_table()
                populate_ibes_table()
            
            if (options["ws"]):
                status = "Currency Price Update"
                update_currency_price_from_dss()
                status = "Daily Ingestion Update"
                daily_migrations()
                # daily_process_ohlcvtr(region_id = "ws")
                daily_ingestion(region_id="ws")
                do_function("special_cases_1")
                do_function("master_ohlcvtr_update")
                status = "Master OHLCVTR Update"
                master_ohlctr_update()
                status = "Master TAC Update"
                master_tac_update()
                status = "Master Multiple Update"
                master_multiple_update()
                status = "Interest Update"
                interest_update()
                dividend_daily_update()
                interest_daily_update()
                status = "Macro Ibes Update"
                populate_macro_table()
                populate_ibes_table()
                
        
            if(options["worldscope"]):
                status = "Worldscope Ingestion"
                update_worldscope_quarter_summary_from_dsws(currency_code=options["currency_code"])

            if(options["fundamentals_score"]):
                status = "Fundamentals Score Ingestion"
                update_fundamentals_score_from_dsws(currency_code=options["currency_code"])
                status = "Fundamentals Quality Update"
                update_fundamentals_quality_value(currency_code=options["currency_code"])

            if(options["quandl"]):
                status = "Quandl Ingestion"
                fill_null_quandl_symbol()
                update_quandl_orats_from_quandl()

            if(options["vix"]):
                status = "VIX Ingestion"
                update_vix_from_dsws()

            if(options["interest"]):
                status = "Interest Ingestion"
                interest_update()
                status = "Dividend Daily Update"
                interest_daily_update()
                status = "Interest Daily Update"
                dividend_daily_update()

            if(options["dividend"]):
                status = "Dividend Ingestion"
                dividend_updated()
                status = "Dividend Daily Update"
                interest_daily_update()
            
            if(options["utc_offset"]):
                status = "UTC Offset Update"
                update_utc_offset_from_timezone()

            if(options["weekly"]):
                do_function("universe_populate")
                status = "Ticker Name Ingestion"
                update_ticker_name_from_dsws()
                do_function("universe_populate")
                status = "Weekly Macro & Ibes Migration"
                weekly_migrations()

            if(options["monthly"]):
                status = "Entity Type Ingestion"
                update_entity_type_from_dsws()
                status = "Lot Size Ingestion"
                update_lot_size_from_dss()
                status = "Currency Code Ingestion"
                update_currency_code_from_dss()
                status = "Industry Ingestion"
                update_industry_from_dsws()
                status = "Company Name Ingestion"
                update_company_desc_from_dsws()
                status = "Worldscope Identifier Ingestion"
                update_worldscope_identifier_from_dsws()
                status = "Ticker Symbol Ingestion"
                update_ticker_symbol_from_dss()
                status = "MIC Ingestion"
                update_mic_from_dss()
                status = "Dividend Ingestion"
                dividend_updated()
                status = "Dividend Daily Update"
                dividend_daily_update()
                status = "Fred Ingestion"
                update_fred_data_from_fred()
                status = "IBES Ingestion"
                update_ibes_data_monthly_from_dsws()
                status = "Macro Ingestion"
                update_macro_data_monthly_from_dsws()

        except Exception as e:
            print("{} : === {} ERROR === : {}".format(dateNow(), status, e))
            