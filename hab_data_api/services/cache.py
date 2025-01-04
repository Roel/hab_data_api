import datetime
import zlib

CACHE = {}


def cache_for(seconds):
    def cache(fn):
        def wrapper(*args, **kwargs):
            fn_hash_base = fn.__name__
            fn_hash_base += str(args)
            fn_hash_base += str(kwargs)
            fn_hash = zlib.crc32(fn_hash_base.encode('utf8')) & 0xffffffff

            timestamp, cache = CACHE.get(fn_hash, (None, None))

            if timestamp is not None and cache is not None \
                    and timestamp >= datetime.datetime.now() - datetime.timedelta(seconds=seconds):
                return cache

            result = fn(*args, **kwargs)
            CACHE[fn_hash] = (datetime.datetime.now(), result)
            return result
        return wrapper
    return cache


class CacheService:
    def __init__(self, app):
        self.app = app

        self.__scheduled_jobs()

    def clear_cache(self):
        CACHE.clear()

    def __scheduled_jobs(self):
        self.app.scheduler.add_job(
            self.clear_cache, 'cron', hour='15', minute='0')
