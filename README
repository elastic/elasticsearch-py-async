Python Elasticsearch Async Client
=================================

This is an adapter for `elasticsearch-py`_ providing a transport layer based on
Python's `asyncio`_ module. All API calls now return a future wrapping the
response.

Sniffing (when requested) is also done via a scheduled coroutine.

Example::

    import asyncio
    from elasticsearch_async import AsyncElasticsearch

    client = AsyncElasticsearch(hosts=['localhost', 'other-host'])

    @asyncio.coroutine
    def print_info():
        info = yield from client.info()
        print(info)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(print_info())
    loop.run_until_complete(client.transport.close())
    loop.close()


Example with SSL Context::

    import asyncio
    from elasticsearch_async import AsyncElasticsearch
    from elasticsearch.connection.http_urllib3 import create_ssl_context

    context = create_ssl_context(cafile="/certs/ca/ca.crt")

    client = AsyncElasticsearch(
        hosts=['elasticsearch-xpack'],
        ssl_context=context,
        http_auth=('elastic', 'changeme')
    )

    @asyncio.coroutine
    def print_info():
        info = yield from client.info()
        print(info)

    loop = asyncio.get_event_loop()
    loop.run_until_complete(print_info())
    loop.run_until_complete(client.transport.close())
    loop.close()



``AsyncElasticsearch`` introduces one extra parameter ``loop`` which can be
used to pass in an event loop you wish the client to use. By default
``asyncio.get_event_loop()`` will be used.

.. _elasticsearch-py: http://elasticsearch-py.rtfd.org/
.. _asyncio: https://docs.python.org/3/library/asyncio.html

Installation
------------

``elasticsearch-async`` is available via PyPI so you can install it using pip::

    pip install elasticsearch-async

License
-------

Copyright 2015 Elasticsearch

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
