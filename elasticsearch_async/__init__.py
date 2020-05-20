import asyncio

from elasticsearch import Elasticsearch

from .connection import AIOHttpConnection
from .transport import AsyncTransport


class AsyncElasticsearch(Elasticsearch):
    def __init__(self, hosts=None, transport_class=AsyncTransport, **kwargs):
        super().__init__(hosts, transport_class=transport_class, **kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc_val, _exc_tb):
        await self.transport.close()
