import asyncio

ensure_future = (getattr(asyncio, 'ensure_future', None) or
                 getattr(asyncio, 'async', None))
