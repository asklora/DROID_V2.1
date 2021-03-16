from config.celery import app
from core.universe.models import Universe,UniverseConsolidated
from core.Clients.models import UniverseClient
from core.user.models import User


@app.task
def get_isin_populate_universe(ticker,user_id):
    user = User.objects.get(id=user_id)
    res_celery = []
    try:
        populate = UniverseConsolidated.ingestion_manager.get_isin_code(ticker=ticker)
        if isinstance(ticker, list):
            for tick in ticker:
                ticker_cons = UniverseConsolidated.objects.filter(origin_ticker=tick).distinct('origin_ticker').get()
                if ticker_cons.use_manual:
                    symbol = ticker_cons.origin_ticker
                else:
                    if ticker_cons.consolidated_ticker:
                        symbol = ticker_cons.consolidated_ticker
                    else:
                        symbol = ticker_cons.origin_ticker

                if populate:
                    relation = UniverseClient.objects.filter(client=user.client_user.client_id,ticker=symbol)
                    if relation.exists():
                        res_celery.append({"result":f"relation {user.client_user.client_id} and {tick} exist"})
                    else:
                        UniverseClient.objects.create(client_id=user.client_user.client_id,ticker_id=symbol)
                        res_celery.append({"result":f"relation {user.client_user.client_id} and {tick} created"})
            return res_celery
        else:
            ticker_cons = UniverseConsolidated.objects.filter(origin_ticker=ticker).distinct('origin_ticker').get()
            if ticker_cons.use_manual:
                symbol = ticker_cons.origin_ticker
            else:
                symbol = ticker_cons.consolidated_ticker
            if populate:
                relation = UniverseClient.objects.filter(client=user.client_user.client_id,ticker=symbol)
                if relation.exists():
                    return {"result":f"relation {user.client_user.client_id} and {ticker} exist"}
                else:
                    UniverseClient.objects.create(client_id=user.client_user.client_id,ticker_id=symbol)
                return {"result":f"relation {user.client_user.client_id} and {ticker} created"}
    except Exception as e:
        return {'err':str(e)}
    
