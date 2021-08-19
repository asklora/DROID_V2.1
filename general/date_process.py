import time
import functools
import pandas as pd
from pytz import timezone
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pandas.tseries.offsets import BDay


def get_time_by_timezone(timezone_location):
    return datetime.now(timezone(timezone_location)).utcoffset() / timedelta(minutes=15)


def droid_start_date():
    return backdate_by_year(4)  # backdate_by_year(12)


def droid_start_date_buffer():
    return backdate_by_year(3)  # backdate_by_year(12)


def dlp_start_date_buffer():
    return backdate_by_year(11)  # backdate_by_year(11)


def dlp_start_date():
    return backdate_by_year(12)  # backdate_by_year(12)


def str_to_date(date_string):
    return datetime.strptime(date_string, "%Y-%m-%d").date()


def date_to_string(date_date):
    return date_date.strftime("%Y-%m-%d")


def time_to_string(date_time):
    return date_time.strftime("%H:%M:%S")


def string_to_time(string_time):
    return datetime.strptime(string_time, "%H:%M:%S").time()


def timeNow():
    return datetime.utcnow().time().strftime("%H:%M:%S")


def dateNow():
    return datetime.now().date().strftime("%Y-%m-%d")


def datetimeNow():
    return dateNow() + " " + timeNow()


def timestampNow():
    return datetime.fromtimestamp(time.time())


def backdate_by_day(day):
    return (datetime.now().date() - relativedelta(days=day)).strftime("%Y-%m-%d")


def backdate_by_week(week):
    return (datetime.now().date() - relativedelta(weeks=week)).strftime("%Y-%m-%d")


def backdate_by_month(month):
    return (datetime.now().date() - relativedelta(months=month)).strftime("%Y-%m-%d")


def backdate_by_year(years):
    return (datetime.now().date() - relativedelta(years=years)).strftime("%Y-%m-%d")


def forwarddate_by_day(day):
    return (datetime.now().date() + relativedelta(days=day)).strftime("%Y-%m-%d")


def forwarddate_by_week(week):
    return (datetime.now().date() + relativedelta(weeks=week)).strftime("%Y-%m-%d")


def forwarddate_by_month(month):
    return (datetime.now().date() + relativedelta(months=month)).strftime("%Y-%m-%d")


def forwarddate_by_year(year):
    return (datetime.now().date() + relativedelta(years=year)).strftime("%Y-%m-%d")


def date_plus_bday(start_date, days=1):
    if type(start_date) == str:
        start_date = (str_to_date(start_date) + BDay(days)).strftime("%Y-%m-%d")
    else:
        start_date = ((start_date) + BDay(days)).strftime("%Y-%m-%d")
    return start_date


def date_minus_bday(start_date, days=1):
    if type(start_date) == str:
        start_date = (str_to_date(start_date) - BDay(days)).strftime("%Y-%m-%d")
    else:
        start_date = ((start_date) - BDay(days)).strftime("%Y-%m-%d")
    return start_date


def BackTimeFormat(days, strip=None):
    time = date.today() - timedelta(days=days)
    if strip:
        timeformat = time.strftime("%Y-%m-%d")
    else:
        timeformat = time.strftime("%Y%m%d")
    return timeformat


def count_date_range_by_month(start, end, month, ascending=False):
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")
    date_range = pd.DataFrame({"date": []}, index=[])
    count = -month
    while True:
        count = count + month
        date_result = end_date - relativedelta(months=count)
        date_range = date_range.append(
            pd.DataFrame({"date": [date_result.date()]}, index=[0])
        )
        if (start_date.month >= date_result.month) and (
            start_date.year >= date_result.year
        ):
            break
    date_range.reset_index(inplace=True)
    if ascending:
        date_range = date_range.sort_values(by="date", ascending=True)
    return date_range["date"]


def timeit(func):
    @functools.wraps(func)
    def newfunc(*args, **kwargs):
        startTime = time.time()
        func(*args, **kwargs)
        elapsedTime = time.time() - startTime
        print(
            "function [{}] finished in {} min".format(
                func.__name__, float(elapsedTime / 60)
            )
        )

    return newfunc


def date_interval(date: datetime) -> str:
    week = date.isocalendar()[1]
    year = date.isocalendar()[0]
    return f"{year}{week}"
