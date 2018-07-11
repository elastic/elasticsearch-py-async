import asyncio

from elasticsearch import Elasticsearch

from .connection import AIOHttpConnection
from .transport import AsyncTransport


class AsyncElasticsearch(Elasticsearch):
    def __init__(self, hosts=None, transport_class=AsyncTransport, **kwargs):
        super().__init__(hosts, transport_class=transport_class, **kwargs)

    @asyncio.coroutine
    def __aenter__(self):
        return self

    @asyncio.coroutine
    def __aexit__(self, _exc_type, _exc_val, _exc_tb):
        yield from self.transport.close()
