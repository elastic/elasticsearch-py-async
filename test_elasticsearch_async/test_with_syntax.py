import asyncio
import sys

from pytest import mark

from elasticsearch_async import AsyncElasticsearch


@mark.skipif(sys.version_info < (3, 5), reason="async with is Python 3.5+")
@mark.asyncio
async def test_with_notation_works(port, server, event_loop):
    async with AsyncElasticsearch(
            hosts=['localhost'], port=port, loop=event_loop) as client:
        info = await client.info()
    assert info
