#!/usr/bin/env python3

import aiohttp
import asyncio
import graphviz
import os
import ssl
import sys

#b0729l56
pid = sys.argv[1]
start_url = os.environ.get('BASE') + 'pid.' + pid + '?format=json'

RSMAP = {'brand':['pips-meta:series-resultset', 'pips-meta:episode-resultset', 'pips-meta:clip-resultset'],
         'series':['pips-meta:series-resultset', 'pips-meta:episode-resultset', 'pips-meta:clip-resultset'],
         'episode':['pips-meta:version-resultset', 'pips-meta:clip-resultset'],
         'clip':['pips-meta:version-resultset'],
         'version':['pips-meta:ondemand-resultset','pips-meta:media_asset-resultset', 'pips-meta:broadcast-resultset'],
         'broadcast':[],
         'media_asset':[],
         'ondemand':[]}
      
EMAP = {'brand': None,
        'series': 'member_of',
        'episode': 'member_of',
        'clip': 'clip_of',
        'version': 'version_of',
        'broadcast': 'broadcast_of',
        'media_asset': 'media_asset_of',
        'ondemand': 'broadcast_of'}

sslcontext = ssl.SSLContext(ssl.PROTOCOL_SSLv23)
sslcontext.load_cert_chain(os.environ.get('CERT'))
conn = aiohttp.TCPConnector(ssl_context=sslcontext)

async def fetch(session, url):
    print('fetching url: {}'.format(url))
    async with session.get(url, proxy=os.environ.get('http_proxy')) as response:
        return await response.json()

async def worker(session, q, g):
    print('worker started')
    while True:
        item = await q.get()
        print('got item: {}'.format(item))
        if item == None:
            q.task_done()
            break
        else:
            data = await fetch(session, item)
            entity = parse_entity(data)
            await add_node(entity['child_of'], entity['pid'], g)
            q.task_done()

async def push(q, item):
    print('pushing item: {}'.format(item))
    await q.put(item)

async def kill_worker(q):
    await q.join()
    await push(q, None)

async def add_node(parent, child, g):
    print('adding node')
    g.node(parent, parent)
    g.node(child, child)
    g.edge(parent, child)

def main():
    max_workers = 5
    q = asyncio.Queue(maxsize=max_workers)
    g = graphviz.Digraph(format='svg')
    with aiohttp.ClientSession(connector=conn) as session:
        loop = asyncio.get_event_loop()
        init_task = loop.create_task(push(q, start_url))
        worker_tasks = [loop.create_task(worker(session, q, g)) for i in range(max_workers)]
        kill_tasks = [loop.create_task(kill_worker(q)) for i in range(max_workers)]
        loop.run_until_complete(asyncio.wait([init_task] + worker_tasks + kill_tasks))
    g.render(os.environ.get('HOME') + '/Tmp/' + pid + '.gv')

# parsing pips responses
def all_entities(data):
    return [entity for entity in data[0][2:]]

def format_url(url):
    return url + '?format=json'

def get_list(entity, key):
    return [i for i in entity[2:] if i[0] == key]

def format_id(id):
    data = {}
    data['id'] = id[2]
    data['type'] = id[1]['type']
    data['authority'] = id[1]['authority']
    return data

def parse_entity(entity):
    rel_type = EMAP[entity[0]]
    data = {}
    data['pid'] = entity[1]['pid']
    data['type'] = entity[0]
    data['child_of'] = [i for i in get_list(entity, rel_type) if isinstance(i, list)][0][2][1]['pid']
    data['title'] = [i for i in get_list(entity, 'title') if isinstance(i, list)][0][2]
    data['ids'] = [format_id(i) for i in get_list(entity, 'ids')[0][2:] if isinstance(i, list)]
    data['links'] = []
    return data

if __name__ == '__main__':
    main()
