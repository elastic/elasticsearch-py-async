import asyncio

from elasticsearch.connection_pool import ConnectionPool, DummyConnectionPool


class AsyncConnectionPool(ConnectionPool):
    def __init__(self, connections, loop, **kwargs):
        self.loop = loop
        super().__init__(connections, **kwargs)

    @asyncio.coroutine
    def close(self):
        await ([conn.close() for conn in self.orig_connections],
                           loop=self.loop)


class AsyncDummyConnectionPool(DummyConnectionPool):
    async def close(self):
        await self.connection.close()
