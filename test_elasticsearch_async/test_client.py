from pytest import mark, raises

from elasticsearch import NotFoundError


@mark.asyncio
async def test_custom_body(server, client):
    server.register_response('/', {'custom': 'body'})
    data = await client.info()

    assert [('GET', '/', '', {})] == server.calls
    assert {'custom': 'body'} == data


@mark.asyncio
async def test_info_works(server, client):
    data = await client.info()

    assert [('GET', '/', '', {})] == server.calls
    assert {'body': '', 'method': 'GET', 'params': {}, 'path': '/'} == data


@mark.asyncio
async def test_ping_works(server, client):
    data = await client.ping()

    assert [('HEAD', '/', '', {})] == server.calls
    assert data is True


@mark.asyncio
async def test_exists_with_404_returns_false(server, client):
    server.register_response('/not-there', status=404)
    data = await client.indices.exists(index='not-there')

    assert data is False


@mark.asyncio
async def test_404_properly_raised(server, client):
    server.register_response('/i/t/42', status=404)
    with raises(NotFoundError):
        await client.get(index='i', doc_type='t', id=42)


@mark.asyncio
async def test_body_gets_passed_properly(client):
    data = await client.index(index='i', doc_type='t', id='42',
                              body={'some': 'data'})
    assert {'body': {'some': 'data'}, 'method': 'PUT', 'params': {},
            'path': '/i/t/42'} == data


@mark.asyncio
async def test_params_get_passed_properly(client):
    data = await client.info(params={'some': 'data'})
    assert {'body': '', 'method': 'GET', 'params': {'some': 'data'},
            'path': '/'} == data
