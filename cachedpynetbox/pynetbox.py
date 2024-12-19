from .nbcache.nbcache import SyncedNetbox
import threading
import requests
import time
import logging
from typing import Any


class pynetbox:
    def __init__(
        self,
        base_uri,
        token,
        offline=False,
        trace=False,
        cachetime=60.0,
        readonly=False,
        quick=False,
        debug=False,
        dbpath=".netbox-v2",
    ):
        self._snb = SyncedNetbox(
            base_uri.replace("api/", ""), token, dbpath, readonly, quick
        )
        self._base_uri = base_uri
        self._token = token
        self._offline = offline
        self._trace = trace
        self._cachetime = cachetime
        self._lagmembers = None
        self._lagmembers_lifetime = 0
        self._lock = threading.Lock()
        
        if debug == False:
            logging.basicConfig(level=30)
        else:
            logging.basicConfig(level=10)

        self._sess = requests.Session()
        if self._token != None:
            self._sess.headers.update(authorization="Token {}".format(self._token))

    def post_api(self, url, **kwargs):
        full_url = self._base_uri + url
        r = self._sess.post(full_url, json=kwargs)
        if r.status_code >= 400:
            raise ValueError(
                "Error from netbox %s, status code %u, text:\n%s\n"
                % (url, r.status_code, r.text)
            )

    def patch_api(self, url, **kwargs):
        full_url = self._base_uri + url
        r = self._sess.patch(full_url, json=kwargs)
        if r.status_code >= 400:
            raise ValueError(
                "Error from netbox %s, status code %u, text:\n%s\n"
                % (url, r.status_code, r.text)
            )

    def int_by_device_name(self, name):
        vc = self._snb.dcim.virtual_chassis.getindex("name", name)
        if len(vc) != 0:
            ret = []
            vc_devices = self._snb.dcim.devices.getindex(
                "virtual_chassis.id", vc[0]["id"]
            )
            for dev in vc_devices:
                ret = ret + self._snb.dcim.interfaces.getindex(
                    "device.name", dev["name"]
                )
            return ret
        return self._snb.dcim.interfaces.getindex("device.name", name)

    def ip_by_int_id(self, iid):
        return self._snb.ipam.ip_addresses.getindex("assigned_object.id", iid)

    def prefixes(self):
        return self._snb.ipam.prefixes.all()

    def vlans(self):
        return self._snb.ipam.vlans.all()

    def devices(self):
        return self._snb.dcim.devices.all()

    def device_types(self):
        return self._snb.dcim.device_types.all()

    def device_type_by_name(self, name):
        return self._snb.dcim.device_types.getindex("name", name)

    def has_poe(self, name):
        result = self._snb.dcim.device_types.getindex("model", name)
        if len(result) == 0:
            return False
        else:
            return result[0]["custom_fields"]["poe_capable"]

    def dev_by_name(self, name):
        return self._snb.dcim.devices.getindex("name", name)

    def dev_by_serial(self, serial):
        return self._snb.dcim.devices.getindex("serial", serial)

    def dev_by_role(self, role):
        return self._snb.dcim.devices.getindex("role.slug", role)

    def dev_by_type(self, typ):
        return self._snb.dcim.devices.getindex("device_type.slug", typ)

    def racks(self):
        return dict([(i["name"], i) for i in self._snb.dcim.racks.all()])

    def lag_members_by_iface(self, iface):
        if self._lagmembers_lifetime < time.time():
            self._lagmembers = None
        if self._lagmembers is None:
            self._lagmembers = {}
            for lag in self._snb.dcim.interfaces.getindex(
                "type.label", "Link Aggregation Group (LAG)"
            ):
                self._lagmembers.setdefault(lag["id"], [])

            for ifloop in self._snb.dcim.interfaces.getindex("lag", Any):
                lag = ifloop.get("lag", None)
                assert lag
                self._lagmembers.setdefault(lag["id"], []).append(ifloop)
            self._lagmembers_lifetime = time.time() + self._cachetime / 2.0
        return self._lagmembers[iface["id"]]

    def updater(self):
        self.int_by_device_name("")
        self.ip_by_int_id(0)
        self.prefixes()
        self.vlans()
        self.devices()
        self.device_types()
        self.device_type_by_name("")
        self.has_poe("")
        self.dev_by_name("")
        self.dev_by_serial("")
        self.dev_by_role("")
        self.dev_by_type("")
        self._snb.dcim.interfaces.getindex("type.label", "Link Aggregation Group (LAG)")
        self._snb.dcim.interfaces.getindex("lag", Any)
        self._snb.extras.object_types.all()
