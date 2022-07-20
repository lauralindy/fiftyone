"""
| Copyright 2017-2022, Voxel51, Inc.
| `voxel51.com <https://voxel51.com/>`_
|
"""
import base64
from typing import Any

from pymongo import MongoClient as _MongoClient

from fiftyone.core._pymongo_proxy.database import Database
from fiftyone.core._pymongo_proxy.change_stream import ClusterChangeStream
from fiftyone.core._pymongo_proxy.command_cursor import CommandCursor
from fiftyone.core._pymongo_proxy.util import (
    PymongoAPIBase,
    PymongoProxyMeta,
    with_doc_and_sig,
)


def _proxy(
    method_name: str,
    instance: "Client",
    *args,
    **kwargs,
) -> Any:
    return instance.request(
        pymongo_attr_name=method_name,
        pymongo_args=args,
        pymongo_kwargs=kwargs,
    )


class Client(
    PymongoAPIBase,
    metaclass=PymongoProxyMeta,
    pymongo_cls=_MongoClient,
    pymongo_method_proxy=_proxy,
):
    """Proxy class for pymongo.MongoClient"""

    def __init__(self, api_endpoint_url: str, access_key: str):
        self._api_endpoint_url = api_endpoint_url
        self._access_key = access_key
        self._closed = False
        self._database_cache = {}

    @property
    def api_endpoint_url(self) -> str:
        return self._api_endpoint_url

    @property
    def api_endpoint_headers(self) -> dict[str, Any]:
        if self._access_key:
            key = base64.b64encode(self._access_key).decode()
        return {"Authorization": f"Basic :{key}"}
        return {}

    def __getattr__(self, name: str) -> "Database":
        if name.startswith("_"):
            raise AttributeError(
                "Client has no attribute %r. To access the %s"
                " database, use client[%r]." % (name, name, name)
            )
        return self.__getitem__(name)

    def __getitem__(self, name: str) -> "Database":
        return self.get_database(self, name)

    def close(self) -> None:
        self._closed = True

    @property
    def address(self) -> None:
        return None

    @property
    def primary(self) -> None:
        return None

    @property
    def secondaries(self) -> None:
        return None

    @property
    def arbiters(self) -> None:
        return None

    @property
    def is_primary(self) -> None:
        return None

    @property
    def is_mongos(self) -> None:
        return None

    @property
    def nodes(self) -> None:
        return None

    def start_session(self):
        raise RuntimeError("sessions not allowed")

    @with_doc_and_sig(_MongoClient.list_databases)
    def list_databases(self, *args, **kwargs):
        return CommandCursor(self, *args, **kwargs)

    @with_doc_and_sig(_MongoClient.get_database)
    def get_database(  # pylint: disable=unused-argument,missing-docstring
        self, name=None, *args, **kwargs
    ) -> "Database":
        if not name:
            # TODO: use default instead of rasisng
            raise ValueError("need a name")

        if name not in self._database_cache:
            self._database_cache[name] = Database(self, name, *args, **kwargs)
        return self._database_cache[name]

    @with_doc_and_sig(_MongoClient.get_default_database)
    def get_default_database(  # pylint: disable=unused-argument,missing-docstring
        self,
        default=None,
        codec_options=None,
        read_preference=None,
        write_concern=None,
        read_concern=None,
    ) -> "Database":
        # TODO Determine return value. possibilities: 'admin', 'fiftyone'
        ...

    @with_doc_and_sig(_MongoClient.watch)
    def watch(self, *args, **kwargs):  # pylint: disable=missing-docstring
        return ClusterChangeStream(self, *args, **kwargs)
