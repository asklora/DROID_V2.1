from general.table_name import get_universe_client_table_name
from general.sql_output import insert_data_to_database, upsert_data_to_database
from core.services.tasks import new_ticker_ingestion
from general.date_process import dateNow
from general.sql_process import do_function
from ingestion.data_from_dsws import populate_universe_consolidated_by_isin_sedol_from_dsws, update_entity_type_from_dsws
import pandas as pd
import numpy as np
from core.djangomodule.general import generate_id
from general.sql_query import get_active_universe_by_created, get_consolidated_universe_data, get_universe_client
from ingestion.data_from_dss import populate_ticker_from_dss
from django.utils import timezone

def populate_ticker_monthly():
    client = "dZzmhmoA" #Client is ASKLORA
    fields = ("uid", "origin_ticker", "use_isin", "use_manual", "created", "source_id", "has_data")
    # universe_consolidated = get_consolidated_universe_data()
    # new_universe1 = populate_ticker_from_dss(index_list=["0#.HSLMI"], manual=1)
    # new_universe2 =  populate_ticker_from_dss(index_list=["0#.SPX", "0#.NDX"], isin=1)
    # new_universe3 = populate_ticker_from_dss(index_list=["0#.SXXE"], isin=1)

    # new_universe = new_universe1.append(new_universe2)
    # new_universe = new_universe.append(new_universe3)
    # new_universe.to_csv("/home/loratech/all_universe.csv")
    # new_universe = new_universe.loc[~new_universe["origin_ticker"].isin(universe_consolidated["origin_ticker"].to_list())]
    # new_universe.to_csv("/home/loratech/new_universe.csv")

    # new_universe = pd.read_csv("/home/loratech/new_universe.csv")
    # new_universe = new_universe[["origin_ticker", "source_id", "use_isin", "use_manual"]]
    # new_universe = new_universe.head(5)
    # new_universe["use_manual"] = np.where(new_universe["use_manual"] == 1, True, False)
    # new_universe["use_isin"] = np.where(new_universe["use_isin"] == 1, True, False)
    # new_universe["uid"] = new_universe["origin_ticker"]
    # for index, row in new_universe.iterrows():
    #     new_universe.loc[index, "uid"] = generate_id(8)
    # new_universe["created"] = timezone.now().date()
    # new_universe["updated"] = timezone.now().date()
    # new_universe["has_data"] = False
    # new_universe["source_id"] = "DSS"
    # new_universe["is_active"] = True
    # new_universe["isin"] = None
    # new_universe["cusip"] = None
    # new_universe["use_cusip"] = False
    # new_universe["sedol"] = None
    # new_universe["use_sedol"] = False
    # new_universe["permid"] = None
    # new_universe["consolidated_ticker"] = new_universe["origin_ticker"]
    # print(new_universe)
    # populate_universe_consolidated_by_isin_sedol_from_dsws(manual_universe=new_universe.loc[new_universe["use_manual"] == True], universe=new_universe.loc[new_universe["use_isin"] == True])
    # print(new_universe)

    
    # do_function("universe_populate")
    ticker = get_active_universe_by_created(created=dateNow())
    universe_client = get_universe_client(client_uid=[client])
    new_universe_client = ticker.loc[~ticker["ticker"].isin(universe_client["ticker"].to_list())][["ticker"]]
    new_universe_client["client_uid"] = client
    new_universe_client["created"] = timezone.now()
    new_universe_client["updated"] = timezone.now()
    print(new_universe_client)
    # insert_data_to_database(new_universe_client, get_universe_client_table_name(), how="append")
    new_ticker_ingestion(ticker["ticker"].to_list())