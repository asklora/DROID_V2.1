from ingestion.firestore_migration import mongo_universe_update
from general.sql_query import get_universe_by_region
from django.core.management.base import BaseCommand
from general.date_process import dateNow, str_to_date
from general.sql_process import do_function
from general.sql_output import fill_null_quandl_symbol, update_ingestion_update_time
from ingestion.master_multiple import master_multiple_update
from ingestion.master_tac import master_tac_update
from ingestion.master_ohlcvtr import master_ohlctr_update
from bot.preprocess import (
    dividend_daily_update, 
    interest_daily_update
)
from ingestion.data_from_timezone import update_utc_offset_from_timezone
from ingestion.data_from_dss import update_data_dss_from_dss, update_ticker_symbol_from_dss
from ingestion.data_from_quandl import update_quandl_orats_from_quandl
from ingestion.data_from_rkd import populate_intraday_latest_price_from_rkd
from ingestion.data_from_dsws import (
    dividend_updated_from_dsws, 
    interest_update_from_dsws, 
    populate_ibes_table, 
    populate_macro_table, 
    update_company_desc_from_dsws,
    update_currency_price_from_dsws,
    update_daily_fundamentals_score_from_dsws,
    update_data_dsws_from_dsws, 
    update_entity_type_from_dsws, 
    update_fred_data_from_fred, 
    update_fundamentals_quality_value, 
    update_fundamentals_score_from_dsws, 
    update_ibes_data_monthly_from_dsws, 
    update_industry_from_dsws, 
    update_macro_data_monthly_from_dsws,
    update_rec_buy_sell_from_dsws, 
    update_ticker_name_from_dsws, 
    update_vix_from_dsws, 
    update_worldscope_identifier_from_dsws, 
    update_worldscope_quarter_summary_from_dsws,
    update_currency_code_from_dsws,
    update_lot_size_from_dsws,
    update_mic_from_dsws,
    worldscope_quarter_report_date_from_dsws)

def split_ticker(currency_code, split=1):
    from general.sql_query import get_active_universe

    ticker = get_active_universe(currency_code=currency_code)["ticker"].to_list()
    if(len(ticker) > 400):
        ticker1 = ticker[0:100]
        ticker2 = ticker[100:200]
        ticker3 = ticker[200:300]
        ticker4 = ticker[300:400]
        ticker5 = ticker[400:]
    elif(len(ticker) > 200):
        ticker1 = ticker[0:100]
        ticker2 = ticker[100:200]
        ticker3 = ticker[200:]
    else:
        ticker1 = ticker[0:]
    try:
        if(split == 1):
            return ticker1
        elif(split == 2):
            return ticker2
        elif(split == 3):
            return ticker3
        elif(split == 4):
            return ticker4
        else:
            return ticker5
    except Exception as e:
        return ticker

class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("-na", "--na", type=bool, help="na", default=False)
        parser.add_argument("-ws", "--ws", type=bool, help="ws", default=False)
        parser.add_argument("-worldscope", "--worldscope", type=bool, help="worldscope", default=False)
        parser.add_argument("-fundamentals_score", "--fundamentals_score", type=bool, help="fundamentals_score", default=False)
        parser.add_argument("-fundamentals_rating", "--fundamentals_rating", type=bool, help="fundamentals_rating", default=False)
        parser.add_argument("-quandl", "--quandl", type=bool, help="quandl", default=False)
        parser.add_argument("-vix", "--vix", type=bool, help="vix", default=False)
        parser.add_argument("-interest", "--interest", type=bool, help="interest", default=False)
        parser.add_argument("-dividend", "--dividend", type=bool, help="dividend", default=False)
        parser.add_argument("-utc_offset", "--utc_offset", type=bool, help="utc_offset", default=False)
        parser.add_argument("-weekly", "--weekly", type=bool, help="weekly", default=False)
        parser.add_argument("-monthly", "--monthly", type=bool, help="monthly", default=False)
        parser.add_argument("-split", "--split", type=int, help="split", default=1)
        parser.add_argument("-currency_code", "--currency_code", nargs="+", help="currency_code", default=None)

        parser.add_argument("-month", "--month", type=bool, help="month", default=False)

    def handle(self, *args, **options):
        d = str_to_date(dateNow())
        d = d.strftime("%d")
        try:
            status = ""
            if (options["na"]):
                status = "Daily Ingestion Update"
                ticker = get_universe_by_region(region_id=["na"])["ticker"].to_list()
                update_data_dss_from_dss(ticker=ticker)
                update_ingestion_update_time('data_dss-na', finish=True)
                update_data_dsws_from_dsws(ticker=ticker)
                update_ingestion_update_time('data_dsws-na', finish=True)
                update_currency_price_from_dsws()
                update_ingestion_update_time('currency-na', finish=True)
                do_function("special_cases_1")
                do_function("master_ohlcvtr_update")
                status = "Master OHLCVTR Update"
                master_ohlctr_update()
                status = "Master TAC Update"
                master_tac_update()
                status = "Master Multiple Update"
                master_multiple_update()
                status = "Fundamentals Ingestion"
                update_ingestion_update_time('data_fundamental_score-na', finish=False)
                update_daily_fundamentals_score_from_dsws(ticker=ticker)        # daily update of mkt_cap
                update_ingestion_update_time('data_fundamental_score-na', finish=True)
                status = "Update AI Score"
                update_fundamentals_quality_value()
                status = "Update Firebase Universe"
                populate_intraday_latest_price_from_rkd(currency_code=["HKD"])
                update_ingestion_update_time('latest_price-na', finish=True)
                mongo_universe_update(currency_code=["HKD"])
                status = "Interest Update"
                interest_update_from_dsws()
                update_ingestion_update_time('data_interest-na', finish=True)
                dividend_daily_update()
                interest_daily_update()
            
            if (options["ws"]):
                status = "Daily Ingestion Update"
                ticker = get_universe_by_region(region_id=["ws"])["ticker"].to_list()
                update_data_dss_from_dss(ticker=ticker)
                update_data_dsws_from_dsws(ticker=ticker)
                update_currency_price_from_dsws()
                do_function("special_cases_1")
                do_function("master_ohlcvtr_update")
                status = "Master OHLCVTR Update"
                master_ohlctr_update()
                status = "Master TAC Update"
                master_tac_update()
                status = "Master Multiple Update"
                master_multiple_update()
                status = "Fundamentals Ingestion"
                update_daily_fundamentals_score_from_dsws(ticker=ticker)
                status = "Update AI Score"
                update_fundamentals_quality_value()
                status = "Interest Update"
                interest_update_from_dsws()
                dividend_daily_update()
                interest_daily_update()
                
        
            if(options["worldscope"]):
                if(d in ["1", "2", "3", "4", "5", "6", "7", "01", "02", "03", "04", "05", "06", "07"]):
                    status = "Worldscope Ingestion"
                    ticker = split_ticker(options["currency_code"], split=options["split"])
                    print(ticker)
                    update_ingestion_update_time('data_worldscope_summary', finish=False)
                    update_worldscope_quarter_summary_from_dsws(ticker=ticker)
                    update_ingestion_update_time('data_worldscope_summary', finish=True)
                    status = "Worldscope Report Date Ingestion"
                    worldscope_quarter_report_date_from_dsws(ticker = ticker)
                else:
                    print(dateNow())
                    print(d)
                    print("Not in First MOnth Days")

            if(options["fundamentals_score"]):
                status = "Fundamentals Score Ingestion"
                ticker = split_ticker(options["currency_code"], split=options["split"])
                print(ticker)
                update_ingestion_update_time('data_fundamental_score', finish=False)
                update_fundamentals_score_from_dsws(ticker=ticker)
                update_ingestion_update_time('data_fundamental_score', finish=True)
                status = "Fundamentals Quality Update"
                update_fundamentals_quality_value()
            
            if(options["fundamentals_rating"]):
                if(options["month"]):
                    if(d in ["1", "2", "3", "4", "5", "6", "7", "01", "02", "03", "04", "05", "06", "07"]):
                        status = "Fundamentals Quality Update"
                        update_fundamentals_quality_value()
                else:
                    status = "Fundamentals Quality Update"
                    update_fundamentals_quality_value()

            if(options["quandl"]):
                status = "Quandl Ingestion"
                fill_null_quandl_symbol()
                update_quandl_orats_from_quandl()
                update_ingestion_update_time('data_quandl', finish=True)

            if(options["vix"]):
                status = "VIX Ingestion"
                update_vix_from_dsws()
                update_ingestion_update_time('data_vix', finish=True)

            if(options["interest"]):
                status = "Interest Ingestion"
                interest_update_from_dsws()
                update_ingestion_update_time('data_interest', finish=True)
                status = "Dividend Daily Update"
                interest_daily_update()
                status = "Interest Daily Update"
                dividend_daily_update()

            if(options["dividend"]):
                status = "Dividend Ingestion"
                dividend_updated_from_dsws()
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
                status = "RECSELL & RECBUY Ingestion"
                update_rec_buy_sell_from_dsws()
                update_ingestion_update_time('universe', finish=True)
                status = "IBES Ingestion"
                update_ingestion_update_time('data_ibes_monthly', finish=True)
                update_ibes_data_monthly_from_dsws()
                update_ingestion_update_time('data_ibes_monthly', finish=True)
                update_ingestion_update_time('data_macro_monthly', finish=True)
                status = "Macro Ingestion"
                update_macro_data_monthly_from_dsws()
                status = "FRED Ingestion"
                update_fred_data_from_fred()
                update_ingestion_update_time('data_macro_monthly', finish=True)
                status = "Universe hotness Ingestion"
                do_function("universe_hotness_update") 

            if(options["monthly"]):
                if(d in ["1", "2", "3", "4", "5", "6", "7", "01", "02", "03", "04", "05", "06", "07"]):
                    update_ingestion_update_time('universe', finish=False)  # start ingestion -> label finish as False
                    status = "Entity Type Ingestion"
                    update_entity_type_from_dsws()
                    status = "Lot Size Ingestion"
                    update_lot_size_from_dsws()
                    status = "Currency Code Ingestion"
                    update_currency_code_from_dsws()
                    status = "Industry Ingestion"
                    update_industry_from_dsws()
                    status = "Company Name Ingestion"
                    update_company_desc_from_dsws()
                    status = "Worldscope Identifier Ingestion"
                    update_worldscope_identifier_from_dsws()
                    status = "Ticker Symbol Ingestion"
                    update_ticker_symbol_from_dss()
                    status = "MIC Ingestion"
                    update_mic_from_dsws()
                    update_ingestion_update_time('universe', finish=True)  # finish ingestion universe -> label finish as True
                    update_ingestion_update_time('data_dividend', finish=False)
                    update_ingestion_update_time('data_dividend_daily_rate', finish=False)
                    status = "Dividend Ingestion"
                    dividend_updated_from_dsws()
                    status = "Dividend Daily Update"
                    dividend_daily_update()
                    update_ingestion_update_time('data_dividend', finish=True)
                    update_ingestion_update_time('data_dividend_daily_rate', finish=True)
                    # status = "Fred Ingestion"
                    # update_fred_data_from_fred()
                    # status = "IBES Ingestion"
                    # update_ibes_data_monthly_from_dsws()
                    # status = "Macro Ingestion"
                    # update_macro_data_monthly_from_dsws()
                else:
                    print(dateNow())
                    print(d)
                    print("Not in First MOnth Days")

        except Exception as e:
            print("{} : === {} ERROR === : {}".format(dateNow(), status, e))
            