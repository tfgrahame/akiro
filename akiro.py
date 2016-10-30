#!/usr/bin/env python3

import aiohttp
import asyncio
import os
import ssl
import sys


start_url = os.environ.get('BASE') + 'pid.' + sys.argv[1] + '?format=json'

sslcontext = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
sslcontext.load_cert_chain(os.environ.get('CERT'))
conn = aiohttp.TCPConnector(ssl_context=sslcontext)

async def fetch(session, url):
    print('fetching url: {}'.format(url))
    async with session.get(url, proxy=os.environ.get('http_proxy')) as response:
        return await response.json()

async def fetcher(session, q):
    print('fetcher started')
    url = await q.get()
    print('got url: {}'.format(url))
    data = await fetch(session, url)
    return data

async def push(q, url):
    print('pushing url: {}'.format(url))
    await q.put(url)

def main():
    max_fetchers = 5
    q = asyncio.Queue(maxsize=max_fetchers)
    with aiohttp.ClientSession(connector=conn) as session:
        loop = asyncio.get_event_loop()
        init_task = loop.create_task(push(q, start_url))
        fetch_tasks = loop.create_task(fetcher(session, q))
        result = loop.run_until_complete(asyncio.wait([init_task] + [fetch_tasks]))
    print(type(result[0]))

# parsing pips responses
def all_entities(data):
    return [entity for entity in data[0][2:]]

def format(url):
    return url + '?format=json'

if __name__ == '__main__':
    main()
