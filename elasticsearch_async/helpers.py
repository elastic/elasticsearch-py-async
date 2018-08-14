import asyncio
import logging
from collections import deque

from elasticsearch.helpers import ScanError


logger = logging.getLogger(__name__)


ensure_future = (getattr(asyncio, 'ensure_future', None) or
                 getattr(asyncio, 'async', None))


class Scan:
    def __init__(self, client, query=None, scroll='5m', raise_on_error=True,
                 preserve_order=False, size=1000, request_timeout=None,
                 clear_scroll=True, scroll_kwargs=None, **kwargs):
        """
        Simple abstraction on top of the
        :meth:`~elasticsearch_async.AsyncElasticsearch.scroll` api - a simple
        iterator that yields all hits as returned by underlining scroll requests.

        By default scan does not return results in any pre-determined order. To
        have a standard order in the returned documents (either by score or
        explicit sort definition) when scrolling, use ``preserve_order=True``. This
        may be an expensive operation and will negate the performance benefits of
        using ``scan``

        Args:
            client (elasticsearch_async.AsyncElasticsearch): async client to use
            query (dict): query body, same as for regular search
            scroll (str): Specify how long a consistent view of the index should be
                maintained for scrolled search
            raise_on_error (bool): if to raise ``ScanError`` when some shards fail.
                ``True`` by default
            preserve_order (): don't set the ``search_type`` to ``scan`` — this will
                cause the scroll to paginate with preserving the order. Note that this
                can be an extremely expensive operation and can easily lead to
                unpredictable results, use with caution  # TODO: type? human-readable doc?
            size (int): Batch size (per shard) for every iteration
            request_timeout (): explicit timeout for each call to ``scan``  # TODO: type?
            clear_scroll (bool): if to delete scroll state in ES after getting
                error or consuming all results. Default is ``True``
            scroll_kwargs (dict): additional kwargs to be passed to
                :meth:`~elasticsearch.Elasticsearch.scroll`
            **kwargs: Any additional keyword arguments will be passed to the initial
                :meth:`~elasticsearch.Elasticsearch.search` call

        Raises:
            ScanError: raised when ``raise_on_error is True`` and any shard fails

        Examples:
            # TODO
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


scan = Scan
