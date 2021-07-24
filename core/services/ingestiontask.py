
from general.sql_query import get_universe_by_region
from config.celery import app
from datasource.rkd import RkdData
from core.services.models import ErrorLog
from celery.schedules import crontab
from config.settings import db_debug

if not db_debug:
    app.conf.beat_schedule={
        
        
        "WS-TRKD-DATA": {
            "task": "core.services.ingestionstask.get_trkd_data_by_region",
            "schedule": crontab(minute=30, hour=00, day_of_week="1-5"),
            "kwargs": {"region": "ws"},
        },
        "NA-TRKD-DATA": {
            "task": "core.services.ingestionstask.get_trkd_data_by_region",
            "schedule": crontab(minute=30, hour=16, day_of_week="1-5"),
            "kwargs": {"region": "na"},
        },

    }

@app.task
def get_trkd_data_by_region(region=None):
    try:
        rkd = RkdData()
        universe = get_universe_by_region(region)['ticker'].tolist()
        rkd.get_rkd_data(universe,save=True)
        return {'region':f'{region}','message':'success'}
    except Exception as e:
        err = ErrorLog.objects.create_log(error_description=f"===  ERROR IN POPULATE TRKD DATA FOR REGION {region} ===",error_message=str(e))
        err.send_report_error()
        return {"err": str(e)}