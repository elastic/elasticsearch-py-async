from pytest import mark, raises

from elasticsearch import NotFoundError

@mark.asyncio
def test_custom_body(server, client):
    server.register_response('/', {'custom': 'body'})
    data = yield from client.info()

    assert [('GET', '/', '', {})] == server.calls
    assert {'custom': 'body'} == data


@mark.asyncio
def test_info_works(server, client):
    data = yield from client.info()

    assert [('GET', '/', '', {})] == server.calls
    assert {'body': '', 'method': 'GET', 'params': {}, 'path': '/'} == data


@mark.asyncio
def test_ping_works(server, client):
    data = yield from client.ping()

    assert [('HEAD', '/', '', {})] == server.calls
    assert data is True


@mark.asyncio
def test_exists_with_404_returns_false(server, client):
    server.register_response('/not-there', status=404)
    data = yield from client.indices.exists(index='not-there')

    assert data is False


@mark.asyncio
def test_404_properly_raised(server, client):
    server.register_response('/i/t/42', status=404)
    with raises(NotFoundError):
        yield from client.get(index='i', doc_type='t', id=42)


@mark.asyncio
def test_body_gets_passed_properly(client):
    data = yield from client.index(index='i', doc_type='t', id='42', body={'some': 'data'})
    assert {'body': {'some': 'data'}, 'method': 'PUT', 'params': {}, 'path': '/i/t/42'} == data


@mark.asyncio
def test_params_get_passed_properly(client):
    data = yield from client.info(params={'some': 'data'})
    assert {'body': '', 'method': 'GET', 'params': {'some': 'data'}, 'path': '/'} == data
