import asyncio

from pytest import mark

from elasticsearch_async import AsyncElasticsearch

@mark.asyncio
def test_sniff_on_start_sniffs(server, event_loop, port, sniff_data):
    server.register_response('/_nodes/_all/http', sniff_data)

    client = AsyncElasticsearch(port=port, sniff_on_start=True, loop=event_loop)

    # sniff has been called in the background
    assert client.transport.sniffing_task is not None
    yield from client.transport.sniffing_task

    assert [('GET', '/_nodes/_all/http', '', {})] == server.calls
    connections = client.transport.connection_pool.connections

    assert 1 == len(connections)
    assert 'http://node1:9200' == connections[0].host
    yield from client.transport.close()

@mark.asyncio
def test_retry_will_work(port, server, event_loop):
    client = AsyncElasticsearch(hosts=['not-an-es-host', 'localhost'], port=port, loop=event_loop, randomize_hosts=False)

    data = yield from client.info()
    assert  {'body': '', 'method': 'GET', 'params': {}, 'path': '/'} == data
    yield from client.transport.close()
