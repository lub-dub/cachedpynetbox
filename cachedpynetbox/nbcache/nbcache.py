import time
import requests
from . import pcache
import pynetbox
from pprint import pprint, pformat
import logging
from typing import Any
import datetime

logger = logging.getLogger("syncednetbox")


class SyncedNetbox(object):
    OBJECTCHANGE_ACTION_CREATE = "create"
    OBJECTCHANGE_ACTION_UPDATE = "update"
    OBJECTCHANGE_ACTION_DELETE = "delete"

    class SyncedDict(object):
        def __init__(self, snb, path):
            super().__init__()
            self._snb = snb
            self._path = path
            self._csid = None

        @property
        def netboxdata(self):
            netboxdata = self._snb._netbox
            for item in self._path:
                netboxdata = getattr(netboxdata, item)
            return netboxdata

        def _update(self, force=False):
            if self._csid == self._snb.changes_lastid() and not force:
                return

            path = ".".join(self._path) + ":"
            changestate = self._snb._cache.get_expiry(path)

            if self._snb._readonly:
                self._csid = changestate["csid"]
                self._allids = set(changestate["allids"])
                return

                # logger.debug('%s no data, special-case fetch...' % path)
            if path == "raw.interfaces:":
                logger.debug("fetching interfcaes")
                csid = self._snb.changes_lastid()
                resp = self._snb._netbox.dcim.interfaces.all()
                # assert resp.status_code == 200
                allitems = list(resp)

                self._allids = set([item["id"] for item in allitems])
                for item in allitems:
                    ipath = "%s:%d" % (".".join(self._path), int(item["id"]))
                    self._snb._cache[ipath] = dict(item)

            elif changestate is None or force:
                logger.debug("%s no data/force, fetching all..." % path)

                csid = self._snb.changes_lastid()
                allitems = list(self.netboxdata.all())
                self._allids = set([item.id for item in allitems])
                for item in allitems:
                    ipath = "%s:%d" % (".".join(self._path), int(item.id))
                    self._snb._cache[ipath] = dict(item)
            else:
                csid = changestate["csid"]
                self._allids = set(changestate["allids"])

                changes = list(self._snb.changes_since(csid))
                if len(changes) > 0:
                    logger.debug(
                        "%s changeset %r -> %r"
                        % (".".join(self._path), csid, changes[-1]["id"])
                    )
                for change in changes:
                    csid = change["id"]
                    objtype = change["changed_object_type"]

                    if objtype == "dcim.cabletermination" and (
                        path.startswith("dcim.") or path.startswith("circuits")
                    ):
                        action = change["action"]["value"]
                        if action == self._snb.OBJECTCHANGE_ACTION_DELETE:
                            change_data = change["prechange_data"]
                            action = self._snb.OBJECTCHANGE_ACTION_UPDATE
                        else:
                            change_data = change["postchange_data"]
                        ct_dict = self._snb._cache[
                                "extras.object_types:%d"
                            % (change_data["termination_type"])
                        ]
                        termination_id_name = (
                            ct_dict["app_label"] + "." + ct_dict["model"]
                        )

                        if ".".join(self._path).startswith(termination_id_name):
                            # We also must update the termination endpoint
                            oid = change_data["termination_id"]
                            logger.debug(
                                "termination endpoint updating %s:%d"
                                % (termination_id_name, oid)
                            )
                        else:
                            continue

                        logger.debug("Parsing %s", change["display"])

                    elif ".".join(self._path) not in [objtype, objtype + "s"]:
                        continue
                    else:
                        oid = change["changed_object_id"]
                        action = change["action"]["value"]

                    if action == self._snb.OBJECTCHANGE_ACTION_DELETE:
                        logger.debug(
                            "cset %r for %s delete %r"
                            % (change["id"], ".".join(self._path), oid)
                        )
                        try:
                            self._allids.remove(oid)
                        except KeyError:
                            pass
                    else:
                        logger.debug(
                            "cset %r for %s update %r"
                            % (change["id"], ".".join(self._path), oid)
                        )
                        self._allids.add(oid)
                        ipath = "%s:%d" % (".".join(self._path), oid)
                        try:
                            del self._snb._cache[ipath]
                        except KeyError:
                            pass

            changestate = {"csid": csid, "allids": list(self._allids)}
            self._csid = csid
            self._snb._cache[path] = changestate

        def __getitem__(self, item):
            self._update()
            path = "%s:%d" % (".".join(self._path), int(item))
            return self._snb._cache[path]

        def getindex(self, index, value):
            self._update()

            basepath = ".".join(self._path)
            path = "%s:by-%s" % (basepath, index)
            idx = self._snb._cache[path]
            if idx["cset"] != self._snb.changes_lastid():
                if self._snb._readonly:
                    logger.error(
                        "index %s attr %s outdated index at %s (current %s)"
                        % (basepath, index, idx["cset"], self._snb.changes_lastid())
                    )
                else:
                    del self._snb._cache[path]
                    idx = self._snb._cache[path]
            if value is None:
                qval = "NONE"
            elif value is Any:
                qval = "ANY"
            else:
                qval = "VAL:%s" % value
            results = [
                self._snb._cache["%s:%d" % (basepath, k)]
                for k in idx["items"].get(qval, [])
            ]
            logger.debug(
                f"index {basepath} attr {index} value {repr(value)} => {len(results)} results"
            )
            return sorted(results, key=lambda i: i["id"])

        def all(self):
            self._update()
            basepath = ".".join(self._path)
            return self._snb._cache.get_batch(basepath, self._allids)

        def refresh(self, oid):
            assert oid != ""
            if oid.startswith("by-"):
                idxfield = oid[3:].split(".")
                index = {
                    "cset": self._snb.changes_lastid(),
                    "items": {},
                }
                for item in self.all():
                    val = item
                    for i in idxfield:
                        if val is None or i not in val or val[i] is None:
                            index["items"].setdefault("NONE", []).append(item["id"])
                            break
                        val = val[i]
                    else:
                        index["items"].setdefault("VAL:%s" % val, []).append(item["id"])
                        index["items"].setdefault("ANY", []).append(item["id"])
                return index

            path = ".".join(self._path) + ":"
            if path == "raw.interfaces:":
                return self._snb._cache.getexpiry(path + "data")[oid]
            return dict(self.netboxdata.get(int(oid)))

    class Accessor(object):
        def __init__(self, snb, path=[]):
            super().__init__()
            self._snb = snb
            self._path = path

        @property
        def path(self):
            return ".".join(self._path)

        def _make(self):
            path = ".".join(self._path)
            if path not in self._snb._dicts:
                self._snb._dicts[path] = self._snb.SyncedDict(self._snb, self._path)
            return self._snb._dicts[path]

        def refresh(self, item):
            return self._make().refresh(item)

        def all(self):
            return self._make().all()

        def getindex(self, index, value):
            return self._make().getindex(index, value)

        def __getitem__(self, item):
            return self._make()[item]

        def __getattr__(self, attr):
            return SyncedNetbox.Accessor(self._snb, self._path + [attr])

        def __repr__(self):
            return "<SyncedNetbox.Accessor %r>" % (self._path)

    def __init__(self, url, token, cachefile="cache.db", readonly=False, quick=False):
        self._dicts = {}
        self._cache = pcache.JsonDictCache(
            cachefile,
            refresh=self.refresh,
            lifetime=7200,
            readonly=readonly,
            quick=quick,
        )
        self._url = url
        self._netbox = pynetbox.api(url=url, token=token, threading=True)
        self._changes = None
        self._changes_ts = None
        self._readonly = readonly

        self._session = requests.Session()
        if token != None:
            self._session.headers.update(authorization="Token {}".format(token))

    def refresh_all(self, path):
        logger.debug(path)

        path = path.split(".")
        p = self
        for pc in path:
            p = getattr(p, pc)

        p._make()._update(force=True)

    def refresh(self, item):
        if ":" not in item:
            return self.refresh_all(item)

        path, oid = item.split(":")
        logger.debug(path)
        if path == "changes" and oid == "last":
            lastchange = self._cache.get_expiry("changes:last")
            logger.debug(lastchange)
            if lastchange is not None and lastchange != 0:
                csid = lastchange
                head = lastchange
                nxid = 0
                while nxid < 4:
                    head += 1
                    try:
                        cset = self._netbox.core.object_changes.get(head)
                        if not cset:
                            nxid += 1
                            continue
                    except pynetbox.core.query.RequestError:
                        nxid += 1
                        continue

                    cset = dict(cset)
                    csid = cset["id"]

                    logger.debug("cset append %r" % csid)
                    self._cache["changes:%d" % csid] = cset
                    nxid = 0
                logger.debug("cset updated to %r" % csid)
            else:
                logger.debug("cset initializing from scratch")
                last_30 = datetime.datetime.utcnow() - datetime.timedelta(minutes=30)
                csets = self._netbox.core.object_changes.filter(time_after=last_30)
                csets = sorted(csets, key=lambda x: x.id)
                csid = None
                for cset in csets:
                    cset = dict(cset)
                    csid = cset["id"]
                    self._cache["changes:%d" % csid] = cset
                    logger.debug("cset initialized at %r" % csid)
            return csid

        elif path == "changes":
            return dict(self._netbox.core.object_changes.get(int(oid)))

        path = path.split(".")
        p = self
        for pc in path:
            p = getattr(p, pc)
        return p.refresh(oid)

    def changes_lastid(self):
        last_changes = self._cache.get_expiry("changes:last", expiry=15.0)
        return int(last_changes) if last_changes else 0

    def changes_since(self, lastid):
        head = self.changes_lastid()
        for i in range(lastid + 1, head + 1):
            item = self._cache.get_expiry("changes:%d" % i)
            if item is not None:
                yield item

    def changes_clear(self):
        del self._cache["changes:last"]

    def __getattr__(self, attr):
        return self.Accessor(self, [attr])
