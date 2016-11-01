#!/usr/bin/env python3

import aiohttp
import asyncio
import os
import ssl
import sys

#b0729l56

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
    while True:
        item = await q.get()
        print('got item: {}'.format(item))
        if item == None:
            q.task_done()
            break
        else:
            data = await fetch(session, item)
            print(parse_entity(data))
            q.task_done()

async def push(q, item):
    print('pushing item: {}'.format(item))
    await q.put(item)

async def kill_fetcher(q):
    await q.join()
    await push(q, None)

def main():
    max_fetchers = 5
    q = asyncio.Queue(maxsize=max_fetchers)
    with aiohttp.ClientSession(connector=conn) as session:
        loop = asyncio.get_event_loop()
        init_task = loop.create_task(push(q, start_url))
        fetch_tasks = [loop.create_task(fetcher(session, q)) for i in range(max_fetchers)]
        kill_tasks = [loop.create_task(kill_fetcher(q)) for i in range(max_fetchers)]
        loop.run_until_complete(asyncio.wait([init_task] + fetch_tasks + kill_tasks))

# parsing pips responses
def all_entities(data):
    return [entity for entity in data[0][2:]]

def format_url(url):
    return url + '?format=json'

def get_list(entity, key):
    return [i for i in entity if (isinstance(i, list) and i[0] == key)]

def format_id(id):
    data = {}
    data['id'] = id[2]
    data['type'] = id[1]['type']
    data['authority'] = id[1]['authority']
    return data

def parse_entity(entity):
    data = {}
    data['type'] = entity[0]
    data['member_of'] = [i for i in get_list(entity, 'member_of') if isinstance(i, list)][0][2][1]['pid']
    data['title'] = [i for i in get_list(entity, 'title') if isinstance(i, list)][0][2]
    data['ids'] = [format_id(i) for i in get_list(entity, 'ids')[0][2:] if isinstance(i, list)]
    data['links'] = []
    return data

if __name__ == '__main__':
    main()
