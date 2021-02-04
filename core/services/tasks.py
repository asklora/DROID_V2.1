from config.celery import app
from core.universe.models import Universe,UniverseConsolidated
from core.Clients.models import UniverseClient
from core.user.models import User


@app.task
def get_isin_populate_universe(ticker,user_id):
    user = User.objects.get(id=user_id)
    try:
        populate = UniverseConsolidated.ingestion_manager.get_isin_code(ticker=ticker)
        if populate:
            relation = UniverseClient.objects.filter(client_id=user.client_user.client_id,ticker_id=ticker)
            if relation.exists():
                return {"result":f"relation {user.client_user.client_id} and {ticker} exist"}
            else:
                ticker_cons = UniverseConsolidated.objects.get(origin_ticker=ticker)
                if ticker_cons.use_manual:
                    symbol = ticker_cons.origin_ticker
                else:
                    symbol = ticker_cons.consolidated_ticker
                UniverseClient.objects.create(client_id=user.client_user.client_id,ticker_id=symbol)
            return {"result":f"relation {user.client_user.client_id} and {ticker} created"}
    except Exception as e:
        return {'err':str(e)}
    
