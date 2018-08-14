from .transport import AsyncTransport
from .connection import AIOHttpConnection

from elasticsearch import Elasticsearch


class AsyncElasticsearch(Elasticsearch):
    def __init__(self, hosts=None, transport_class=AsyncTransport, **kwargs):
        super().__init__(hosts, transport_class=transport_class, **kwargs)
