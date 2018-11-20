from json import dumps

import pytest
from elasticsearch.helpers import ScanError

from elasticsearch_async.helpers import scan
from test_elasticsearch_async import required_python

pytestmark = [
    pytest.mark.asyncio,
    required_python(3, 5)
]


@pytest.fixture(autouse=True)
async def es_data(es):
    actions = [
        {'index': {'_id': 1}}, {'field': 'a'},
        {'index': {'_id': 2}}, {'field': 'b'},
        {'index': {'_id': 3}}, {'field': 'c'},
        {'index': {'_id': 4}}, {'field': 'd'},
        {'index': {'_id': 5}}, {'field': 'e'},
        {'index': {'_id': 6}}, {'field': 'f'},
        {'index': {'_id': 7}}, {'field': 'g'},
        {'index': {'_id': 8}}, {'field': 'h'},
    ]
    await es.bulk(
        body='\n'.join(dumps(row) for row in actions),
        index='test_scan',
        doc_type='doc',
        refresh=True,
    )


async def test_basic(es, mocker):
    mocker.spy(es.transport, 'perform_request')

    docs = []
    async for d in scan(es, size=5):
        docs.append(d)
    assert len(docs) == 8
    assert {d['_id'] for d in docs} == set(map(str, range(1, 9)))
    assert {d['_source']['field'] for d in docs} == set('abcdefgh')

    assert es.transport.perform_request.call_args_list == [
        (('GET', '/_search'), mocker.ANY),            # initial search
        (('GET', '/_search/scroll'), mocker.ANY),     # scan
        (('GET', '/_search/scroll'), mocker.ANY),     # empty scan
        (('DELETE', '/_search/scroll'), mocker.ANY),  # scroll cleanup
    ]


async def _active_scrolls(es):
    raw = await es.cat.nodes(h='search.scroll_current')
    return int(raw.strip('\n'))


async def test_cleanup(es):
    assert await _active_scrolls(es) == 0

    async for _ in scan(es, clear_scroll=True):
        pass
    assert await _active_scrolls(es) == 0

    scroller = scan(es, clear_scroll=False)
    try:
        async for _ in scroller:
            pass
        assert await _active_scrolls(es) != 0
    finally:
        await es.clear_scroll(scroll_id=scroller._scroll_id)


async def test_scan_error(es, mocker):
    def failing_shard(func):
        async def wrapper(*args, **kwargs):
            response = await func(*args, **kwargs)
            if '_shards' in response:
                response['_shards']['successful'] -= 1
                response['_shards']['failed'] += 1
            return response
        return wrapper

    res = []
    async for d in scan(es):
        res.append(d)
    assert len(res) == 8

    warning_mock = mocker.patch('elasticsearch_async.helpers.logger.warning')
    mocker.patch.object(
        es.transport,
        'perform_request',
        wraps=failing_shard(es.transport.perform_request)
    )

    with pytest.raises(ScanError):
        async for _ in scan(es):
            pass
    assert warning_mock.called
    assert await _active_scrolls(es) == 0

    mocker.resetall()
    res = []
    async for d in scan(es, raise_on_error=False):
        res.append(d)
    assert len(res) == 8
    assert warning_mock.called
    assert await _active_scrolls(es) == 0

