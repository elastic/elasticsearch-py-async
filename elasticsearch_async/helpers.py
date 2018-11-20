import asyncio
import logging
from collections import deque

from elasticsearch.helpers import ScanError

logger = logging.getLogger(__name__)


ensure_future = (getattr(asyncio, 'ensure_future', None) or
                 getattr(asyncio, 'async', None))


class _Scan:
    def __init__(self, client, query=None, scroll='5m', raise_on_error=True,
                 preserve_order=False, size=1000, request_timeout=None,
                 clear_scroll=True, scroll_kwargs=None, **kwargs):
        """
        Simple abstraction on top of the
        :meth:`~elasticsearch_async.AsyncElasticsearch.scroll` api - a simple
        iterator that yields all hits as returned by underlining scroll requests.
        For the sake of naming compatibility with sync library, public interface
        is ``elasticsearch_async.helpers.scan``

        Args:
            client (elasticsearch_async.AsyncElasticsearch): async client to use
            query (dict): query body, same as for regular search
            scroll (str): Specify how long a consistent view of the index should be
                maintained for scrolled search
            raise_on_error (bool): if to raise ``ScanError`` when some shards fail.
                ``True`` by default
            preserve_order (bool): if to preserve sorting from query.
                If set to ``False``, documents will be sorted by ``doc`` —
                this leverages internal optimization for faster scrolling.
                Setting this to ``True`` may be extremely expensive and negate
                benefits of scrolling. ``False`` by default
            size (int): Batch size (per shard) for every iteration
            request_timeout (str): explicit timeout for each call to ``scan``.
              Must correspond ``Time units`` format, look to
              https://www.elastic.co/guide/en/elasticsearch/reference/current/common-options.html#time-units
              for details
            clear_scroll (bool): if to delete scroll state in ES after getting
                error or consuming all results. Default is ``True``
            scroll_kwargs (dict): additional kwargs to be passed to
                :meth:`~elasticsearch.Elasticsearch.scroll`
            **kwargs: Any additional keyword arguments will be passed to the initial
                :meth:`~elasticsearch.Elasticsearch.search` call

        Raises:
            ScanError: raised when ``raise_on_error is True`` and any shard fails

        Examples:
            Scan over all documents::
                aes = AsyncElasticsearch()
                async for doc in scan(aes): ...

            Get all documents from ``async`` index::
                async_docs = [d async for d in scan(aes, index='async')]

            Filter documents by ``topic`` term::
                async_gen = scan(
                    aes,
                    query={'query': {'term': {'topic': 'async'}}})
                async_docs = [d async for d in async_gen]

            Get all documents, fetch documents 100 at time, ignore failures::
                async for d in scan(aes, size=100, raise_on_error=False): ...
        """
        self.client = client
        self.query = query or {}
        if not preserve_order:
            self.query = self.query.copy()
            self.query["sort"] = "_doc"
        self.scroll = scroll
        self.raise_on_error = raise_on_error
        self.size = size
        self.request_timeout = request_timeout
        self.clear_scroll = clear_scroll
        self.scroll_kwargs = scroll_kwargs or {}
        self.kwargs = kwargs

        self._hits = deque()
        self._scroll_id = None

    def __aiter__(self):
        return self

    async def _clear(self):
        if self._scroll_id and self.clear_scroll:
            await self.client.clear_scroll(
                body={'scroll_id': [self._scroll_id]},
                ignore=(404,))

    async def __anext__(self):
        if not self._hits:
            try:
                await self._fetch()
            except ScanError:
                await self._clear()
                raise

        try:
            return self._hits.popleft()
        except IndexError:
            await self._clear()
            raise StopAsyncIteration

    async def _fetch(self):
        if self._scroll_id is None:
            resp = await self.client.search(
                body=self.query,
                scroll=self.scroll,
                size=self.size,
                request_timeout=self.request_timeout,
                **self.kwargs)
        else:
            resp = await self.client.scroll(
                self._scroll_id,
                scroll=self.scroll,
                request_timeout=self.request_timeout,
                **self.scroll_kwargs)

        self._scroll_id = resp.get('_scroll_id')

        if resp['_shards']['successful'] < resp['_shards']['total']:
            logger.warning(
                'Scroll request has only succeeded on %d shards out of %d.',
                resp['_shards']['successful'], resp['_shards']['total'])
            if self.raise_on_error:
                raise ScanError(
                    self._scroll_id,
                    'Scroll request has only succeeded on %d shards out of %d.' %
                    (resp['_shards']['successful'], resp['_shards']['total']))

        self._hits.extend(resp['hits']['hits'])


scan = _Scan
