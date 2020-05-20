from pytest import mark

from elasticsearch_async import AsyncElasticsearch
from elasticsearch_async.connection_pool import \
    AsyncConnectionPool, AsyncDummyConnectionPool


@mark.asyncio
async def test_single_host_makes_async_dummy_pool(server, client, event_loop, port):
    client = AsyncElasticsearch(port=port, loop=event_loop)
    assert isinstance(client.transport.connection_pool, AsyncDummyConnectionPool)
    await client.transport.close()

@mark.asyncio
async def test_multiple_hosts_make_async_pool(server, event_loop, port):
    client = AsyncElasticsearch(
        hosts=['localhost', 'localhost'], port=port, loop=event_loop)
    assert isinstance(client.transport.connection_pool, AsyncConnectionPool)
    assert len(client.transport.connection_pool.orig_connections) == 2
    await client.transport.close()

@mark.asyncio
async def test_async_dummy_pool_is_closed_properly(server, event_loop, port):
    client = AsyncElasticsearch(port=port, loop=event_loop)
    assert isinstance(client.transport.connection_pool, AsyncDummyConnectionPool)
    await client.transport.close()
    assert client.transport.connection_pool.connection.session.closed

@mark.asyncio
async def test_async_pool_is_closed_properly(server, event_loop, port):
    client = AsyncElasticsearch(
        hosts=['localhost', 'localhost'], port=port, loop=event_loop)
    assert isinstance(client.transport.connection_pool, AsyncConnectionPool)
    await client.transport.close()
    for conn in client.transport.connection_pool.orig_connections:
        assert conn.session.closed
