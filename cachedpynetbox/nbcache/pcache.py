import time

try:
    import ujson as json
except ImportError:
    import json
import dbm
import dbm.gnu
import threading
import logging

logger = logging.getLogger("jsondictcache")


class NotInCache(Exception):
    pass


class JsonDictCache(object):
    def __init__(
        self, path, refresh, lifetime, readonly=False, quick=False, quick_lifetime=60
    ):
        super().__init__()
        self.path = path
        self.refresh = refresh
        self.lifetime = lifetime
        self.lock = threading.Lock()
        self.readonly = readonly
        self.quick = bool(quick)
        self.db_open_since = time.time()
        self.semi_quick = quick == "semi"
        self.semi_quick_lifetime = quick_lifetime

        # ensure db location is usable
        if readonly:
            with dbm.gnu.open(self.path, "ru") as testopen:
                pass
        else:
            with dbm.gnu.open(self.path, "c") as testopen:
                pass

        self.db = None
        self.ensure_open_db()

    def ensure_open_db(self):
        with self.lock:
            if self.db is not None:
                # TODO: The really clever solution here would be to check if the inode has changed
                # (ie. was replaced by updater process)
                if self.semi_quick and time.time() > (
                    self.db_open_since + self.semi_quick_lifetime
                ):
                    self.db.close()
                else:
                    return
            if self.quick and not self.readonly:
                self.db = dbm.gnu.open(self.path, "cs")
            elif self.quick and self.readonly:
                self.db = dbm.gnu.open(self.path, "ru")
            self.db_open_since = time.time()

    def get_expiry(self, item, default=None, expiry=None, cacheonly=False):
        self.ensure_open_db()
        with self.lock:
            if self.quick:
                try:
                    value = json.loads(self.db.get(item, b"{}").decode("UTF-8"))
                except (UnicodeDecodeError, json.decoder.JSONDecodeError):
                    del self.db[item]
                    value = "{}"
            else:
                with dbm.gnu.open(self.path, "ru" if self.readonly else "c") as db:
                    try:
                        value = json.loads(db.get(item, b"{}").decode("UTF-8"))
                    except (UnicodeDecodeError, json.decoder.JSONDecodeError):
                        del db[item]
                        value = "{}"

        if expiry is None or self.readonly:
            # logger.debug(f'{repr(item)} unchecked')
            return value.get("data", default)

        ts = value.get("ts", 0)
        if time.time() - expiry < ts:
            # logger.debug(f'{repr(item)} cached')
            return value["data"]

        if cacheonly:
            raise NotInCache()

        logger.debug("%r refreshing" % item)
        data = self.refresh(item)
        self[item] = data
        return data

    def get_batch(self, path, ids):
        missing = set()
        items = []
        MISSING_THRESHOLD = 50

        for id_ in ids:
            full_id = "%s:%d" % (path, id_)
            try:
                items.append(
                    self.get_expiry(full_id, None, self.lifetime, cacheonly=True)
                )
            except NotInCache:
                missing.add(id_)
                if len(missing) >= MISSING_THRESHOLD:
                    break
        else:
            if missing:
                logger.debug(
                    "%s: missing %d items, fetching individually", path, len(missing)
                )
                for miss in missing:
                    full_id = "%s:%d" % (path, miss)
                    items.append(self.get_expiry(full_id, None, self.lifetime))
            return items

        logger.debug("%s: missing >= %d items, using bulk fetch", path, len(missing))
        self.refresh(path)
        return [self["%s:%d" % (path, id_)] for id_ in ids]

    def __getitem__(self, item):
        return self.get_expiry(item, None, self.lifetime)

    def __setitem__(self, item, data):
        if self.readonly:
            raise IOError("cache opened in readonly mode")
        with self.lock:
            if self.quick:
                self.db[item] = json.dumps({"ts": time.time(), "data": data}).encode(
                    "UTF-8"
                )
            else:
                with dbm.gnu.open(self.path, "c") as db:
                    db[item] = json.dumps({"ts": time.time(), "data": data}).encode(
                        "UTF-8"
                    )

    def __delitem__(self, item):
        if self.readonly:
            raise IOError("cache opened in readonly mode")
        with self.lock:
            if self.quick:
                del self.db[item]
            else:
                with dbm.gnu.open(self.path, "c") as db:
                    del db[item]
