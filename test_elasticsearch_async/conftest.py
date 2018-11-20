import asyncio
import json
import logging
import time

import aiohttp.web
from elasticsearch import Elasticsearch
from pytest import fixture, yield_fixture

from elasticsearch_async import AIOHttpConnection, AsyncElasticsearch


@yield_fixture
def connection(event_loop, server, port):
    connection = AIOHttpConnection(port=port, loop=event_loop)
    yield connection
    event_loop.run_until_complete(connection.close())


class DummyElasticsearch(aiohttp.web.Server):

    def __init__(self, **kwargs):
        super().__init__(handler=self.handler, **kwargs)
        self._responses = {}
        self.calls = []

    def register_response(self, path, response={}, status=200):
        self._responses[path] = status, response

    @asyncio.coroutine
    def handler(self, request):
        url = request.url

        params = dict(request.query)
        body = yield from request.read()
        body = json.loads(body.decode('utf-8')) if body else ''

        self.calls.append((request.method, url.path, body, params))

        if url.path in self._responses:
            status, body = self._responses.pop(url.path)
            if asyncio.iscoroutine(body):
                body = yield from body
        else:
            status = 200
            body = {
                'method': request.method,
                'params': params,
                'path': url.path,
                'body': body
            }

        out = json.dumps(body)

        return aiohttp.web.Response(body=out, status=status, content_type='application/json')


i = 0
@fixture
def port():
    global i
    i += 1
    return 8080 + i

@yield_fixture
def server(event_loop, port):
    server = DummyElasticsearch(debug=True, loop=event_loop)
    f = event_loop.create_server(server, '127.0.0.1', port)
    event_loop.run_until_complete(f)
    yield server
    event_loop.run_until_complete(server.shutdown(timeout=.5))

@yield_fixture
def client(event_loop, server, port):
    logger = logging.getLogger('elasticsearch')
    logger.setLevel(logging.DEBUG)
    c = AsyncElasticsearch([{'host': '127.0.0.1','port': port}], loop=event_loop)
    yield c
    event_loop.run_until_complete(c.transport.close())

@fixture
def sniff_data():
    return {
        "ok" : True,
        "cluster_name" : "super_cluster",
        "nodes" : {
            "node1" : {
                "name": "Thunderbird",
                "transport_address": "node1/127.0.0.1:9300",
                "host": "node1",
                "ip": "127.0.0.1",
                "version": "2.1.0",
                "build": "72cd1f1",
                "http" : {
                    "bound_address" : [ "[fe80::1]:9200", "[::1]:9200", "127.0.0.1:9200" ],
                    "publish_address" : "node1:9200",
                    "max_content_length_in_bytes" : 104857600
                },
                "attributes": {
                    "testattr": "test"
                }
            }
        }
    }


@fixture(scope='session')
def es_available():
    """
    This fixture is a hack to checking ES server availability.
    Seems that it's better to make it via sync client once per session instead
    of making check for every test case or patching ``event_loop`` fixture
    """
    timeout = 10  # seconds
    es = Elasticsearch()
    for _ in range(timeout * 10):
        try:
            es.ping()
            return
        except ConnectionError:
            time.sleep(timeout / 10)


@fixture
def es(event_loop, es_available):
    es = AsyncElasticsearch(loop=event_loop)
    event_loop.run_until_complete(es.indices.delete('test_*', ignore=404))
    yield es
    event_loop.run_until_complete(es.transport.close())
