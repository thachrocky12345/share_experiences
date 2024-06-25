from copy import deepcopy
from date_ext import timestamp
from datetime import datetime


def get_timestamp_seconds():
    return timestamp(datetime.now())

CACHE_TIME = 30
MAX_CACHE = 10000


class CacheExeError(Exception):
    pass

def datetimetz(datetime_stamp, tz=psycopg2.tz.FixedOffsetTimezone()):
    return datetime_stamp.replace(tzinfo=tz)

def totalseconds(datetime_ts):
    if not isinstance(datetime_ts, timedelta):
        raise TypeError("datetime_ts argument must be a timedelta, not {0}".format(type(datetime_ts)))
    return (datetime_ts.microseconds + (datetime_ts.seconds + datetime_ts.days * 24 * 3600) * 1000000) / 1000000  # python 2.7 can use the total seconds method


def timestamp(datetime_ts):
    datetime_ts = datetimetz(datetime_ts)
    d = datetime_ts - datetimetz(datetime(1970, 1, 1))
    return totalseconds(d)

class CacheData(object):

    def __init__(self, cache_data=None, cache_time=CACHE_TIME, max_cache_number=MAX_CACHE):
        self.cache_time = cache_time
        self.max_cache_number = max_cache_number
        if cache_data is None:
            self.data = {}
        else:
            self.data = cache_data

    def get_cache(self, cache_id):
        result = self.data.get(cache_id)
        if result:
            try:
                if get_timestamp_seconds() - result[1] < self.cache_time:
                    return result[0]
                else:
                    self.data.pop(cache_id)

            except Exception as err:
                raise CacheExeError(err.message)


    def set_cache(self, cache_id, value):
        if len(self.data) > self.max_cache_number:
            self.data.clear()
        # since result can be list and dict it is mutable value, we need a deepcopy save the record
        self.data[cache_id] = deepcopy((value, get_timestamp_seconds()))
