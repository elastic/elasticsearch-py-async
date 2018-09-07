import asyncio

from elasticsearch import ConnectionPool
from elasticsearch.connection_pool import DummyConnectionPool


class AsyncConnectionPool(ConnectionPool):
    def __init__(self, connections, loop, **kwargs):
        self.loop = loop
        super().__init__(connections, **kwargs)

    async def close(self):
        await asyncio.wait([conn.close() for conn in self.orig_connections],
                           loop=self.loop)


class AsyncDummyConnectionPool(DummyConnectionPool):
    async def close(self):
        await self.connection.close()
