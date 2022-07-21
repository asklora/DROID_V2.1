from ingestion.firestore_migration import firebase_universe_update
from general.sql_query import get_universe_by_region
from django.core.management.base import BaseCommand
from general.date_process import dateNow, str_to_date
from general.sql_process import do_function
from general.sql_output import activate_position_ticker, fill_null_quandl_symbol
# from ingestion.master_multiple import master_multiple_update
# from ingestion.master_tac import master_tac_update
# from ingestion.master_ohlcvtr import master_ohlctr_update
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
    update_company_desc_from_dsws,
    update_currency_price_from_dsws,
    update_daily_fundamentals_score_from_dsws,
    update_data_dsws_from_dsws, 
    update_entity_type_from_dsws, 
    update_fred_data_from_fred, 
    update_fundamentals_quality_value, 
    update_fundamentals_score_from_dsws_multi,
    update_ibes_data_monthly_from_dsws, 
    update_industry_from_dsws, 
    update_macro_data_monthly_from_dsws,
    update_rec_buy_sell_from_dsws, 
    update_ticker_name_from_dsws, 
    update_vix_from_dsws, 
    update_worldscope_identifier_from_dsws, 
    update_worldscope_quarter_summary_from_dsws_multi,
    update_currency_code_from_dsws,
    update_lot_size_from_dsws,
    update_mic_from_dsws)

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
        parser.add_argument("-currency_price", "--currency_price", type=bool, help="currency_price", default=False)
        parser.add_argument("-dividend", "--dividend", type=bool, help="dividend", default=False)
        parser.add_argument("-utc_offset", "--utc_offset", type=bool, help="utc_offset", default=False)
        parser.add_argument("-weekly", "--weekly", type=bool, help="weekly", default=False)
        parser.add_argument("-monthly", "--monthly", type=bool, help="monthly", default=False)
        parser.add_argument("-split", "--split", type=int, help="split", default=1)
        parser.add_argument("-currency_code", "--currency_code", nargs="+", help="currency_code", default=None)
        parser.add_argument("-month", "--month", type=bool, help="month", default=False)
        parser.add_argument("-ai_rating", "--ai_rating", type=bool, help="ai_rating", default=False)
        parser.add_argument("-firebase_universe", "--firebase_universe", type=bool, help="firebase_universe", default=False)


    def handle(self, *args, **options):
        d = str_to_date(dateNow())
        d = d.strftime("%d")
        try:
            status = ""
            if (options["na"]):
                activate_position_ticker()
                ticker = get_universe_by_region(region_id=["na"])["ticker"].to_list()
                if(options["ai_rating"]):
                    status = "Fundamentals Ingestion"
                    update_daily_fundamentals_score_from_dsws(ticker=ticker)
                    status = "Update AI Score"
                    update_fundamentals_quality_value()
                elif(options["firebase_universe"]):
                    status = "Update Firebase Universe"
                    firebase_universe_update(currency_code=["HKD"])
                else:
                    update_data_dss_from_dss(ticker=ticker)
                    update_data_dsws_from_dsws(ticker=ticker)
                    do_function("special_cases_1")
                    do_function("master_ohlcvtr_update")
                    status = "Master OHLCVTR Update"
                    master_ohlctr_update()
                    status = "Master TAC Update"
                    master_tac_update()
                    status = "Master Multiple Update"
                    master_multiple_update()
                    status = "Interest Update"
                    interest_update_from_dsws()
                    dividend_daily_update()
                    interest_daily_update()
            if (options["ws"]):
                activate_position_ticker()
                ticker = get_universe_by_region(region_id=["ws"])["ticker"].to_list()
                if(options["ai_rating"]):
                    status = "Fundamentals Ingestion"
                    update_daily_fundamentals_score_from_dsws(ticker=ticker)
                    status = "Update AI Score"
                    update_fundamentals_quality_value()
                elif(options["firebase_universe"]):
                    status = "Update Firebase Universe"
                    firebase_universe_update(currency_code=["USD"])
                else:
                    status = "Daily Ingestion Update"
                    update_data_dss_from_dss(ticker=ticker)
                    update_data_dsws_from_dsws(ticker=ticker)
                    do_function("special_cases_1")
                    do_function("master_ohlcvtr_update")
                    status = "Master OHLCVTR Update"
                    master_ohlctr_update()
                    status = "Master TAC Update"
                    master_tac_update()
                    status = "Master Multiple Update"
                    master_multiple_update()
                    status = "Interest Update"
                    interest_update_from_dsws()
                    dividend_daily_update()
                    interest_daily_update()
                    
            if (options["currency_price"]):
                status = "Currency Price Update"
                update_currency_price_from_dsws()

            if(options["worldscope"]):      # change to weekly but only missing
                status = "Worldscope Ingestion"
                update_worldscope_quarter_summary_from_dsws_multi(split=options["split"], ticker=None)  # now report_date ingestion is also included

            if(options["fundamentals_score"]):
                status = "Fundamentals Score Ingestion"
                update_fundamentals_score_from_dsws_multi(ticker=None)
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

            if(options["vix"]):
                status = "VIX Ingestion"
                update_vix_from_dsws()

            if(options["interest"]):
                status = "Interest Ingestion"
                interest_update_from_dsws()
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
                status = "IBES Ingestion"
                update_ibes_data_monthly_from_dsws()
                status = "Macro Ingestion"
                update_macro_data_monthly_from_dsws()
                status = "FRED Ingestion"
                update_fred_data_from_fred()
                status = "Universe hotness Ingestion"
                do_function("universe_hotness_update") 

            if(options["monthly"]):
                if(d in ["1", "2", "3", "4", "5", "6", "7", "01", "02", "03", "04", "05", "06", "07"]):
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
                    status = "Dividend Ingestion"
                    dividend_updated_from_dsws()
                    status = "Dividend Daily Update"
                    dividend_daily_update()
                else:
                    print(dateNow())
                    print(d)
                    print("Not in First MOnth Days")

        except Exception as e:
            print("{} : === {} ERROR === : {}".format(dateNow(), status, e))
            