import asyncio
import json
import logging

import aiohttp

from pytest import mark, yield_fixture, raises

from elasticsearch import NotFoundError, ConnectionTimeout

from elasticsearch_async.connection import AIOHttpConnection

@mark.asyncio
def test_info(connection):
    status, headers, data = yield from connection.perform_request('GET', '/')

    data = json.loads(data)

    assert status == 200
    assert  {'body': '', 'method': 'GET', 'params': {}, 'path': '/'} == data

def test_auth_is_set_correctly(event_loop):
    connection = AIOHttpConnection(http_auth=('user', 'secret'), loop=event_loop)
    assert connection.session._default_auth == aiohttp.BasicAuth('user', 'secret')

    connection = AIOHttpConnection(http_auth='user:secret', loop=event_loop)
    assert connection.session._default_auth == aiohttp.BasicAuth('user', 'secret')

@mark.asyncio
def test_request_is_properly_logged(connection, caplog, port, server):
    server.register_response('/_cat/indices', {'cat': 'indices'})
    yield from connection.perform_request('GET', '/_cat/indices', body=b'{}', params={"format": "json"})

    for logger, level, message in caplog.record_tuples:
        if logger == 'elasticsearch' and level == logging.INFO:
            assert message.startswith('GET http://localhost:%s/_cat/indices?format=json [status:200 request:' % port)
            break
    else:
        assert False, 'Message not found'

    assert ('elasticsearch', logging.DEBUG, '> {}') in caplog.record_tuples
    assert ('elasticsearch', logging.DEBUG, '< {"cat": "indices"}') in caplog.record_tuples

@mark.asyncio
def test_error_is_properly_logged(connection, caplog, port, server):
    server.register_response('/i', status=404)
    with raises(NotFoundError):
        yield from connection.perform_request('GET', '/i', params={'some': 'data'})

    for logger, level, message in caplog.record_tuples:
        if logger == 'elasticsearch' and level == logging.WARNING:
            assert message.startswith('GET http://localhost:%s/i?some=data [status:404 request:' % port)
            break
    else:
        assert False, "Log not received"

@mark.asyncio
def test_timeout_is_properly_raised(connection, server):
    @asyncio.coroutine
    def slow_request():
        yield from asyncio.sleep(1)
        return {}
    server.register_response('/_search', slow_request())

    with raises(ConnectionTimeout):
        yield from connection.perform_request('GET', '/_search', timeout=0.0001)


def test_dns_cache_is_enabled_by_default(event_loop):
    connection = AIOHttpConnection(loop=event_loop)
    assert connection.session.connector.use_dns_cache is True


def test_dns_cache_can_be_disabled(event_loop):
    connection = AIOHttpConnection(loop=event_loop, use_dns_cache=False)
    assert connection.session.connector.use_dns_cache is False
