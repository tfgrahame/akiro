#!/usr/bin/env python3

import aiohttp
import asyncio
import graphviz
import os
import ssl
import sys

pid = sys.argv[1]
start_url = os.environ.get('PIPS_BASE') + 'pid.' + pid + '?format=json'

MAP = {'brand':{'rel_type':'tleo', 'urls':['children/series', 'children/episodes', 'children/clips'], 'shape':'ellipse'},
        'series':{'rel_type':'member_of', 'urls':['children/series', 'children/episodes', 'children/clips'], 'shape':'ellipse'},
        'episode':{'rel_type':'member_of', 'urls':['children/versions', 'children/clips'], 'shape':'ellipse'},
        'clip':{'rel_type':'clip_of', 'urls':['versions'], 'shape':'ellipse'},
        'version':{'rel_type':'version_of', 'urls':[], 'shape':'diamond'},
        'broadcast':{'rel_type':'broadcast_of', 'urls':[], 'shape':'square'},
        'media_asset':{'rel_type':'media_asset_of', 'urls':[], 'shape':'octagon'},
        'ondemand':{'rel_type':'broadcast_of', 'urls':[], 'shape':'circle'}}

suffix = ''
if len(sys.argv) > 2 and sys.argv[2] == '--all':
    MAP['version']['urls'] = ['ondemands', 'media_assets', 'broadcasts']
    suffix = '-all'

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
            response = await fetch(session, item)
            data = parse_response(response)
            for entity in data:
                if entity['type'] in MAP:
                    await add_node(entity, g)
                for link in entity['links']:
                    await push(q, link)
            q.task_done()

async def push(q, item):
    print('pushing item: {}'.format(item))
    await q.put(item)
    print('queue size:', q.qsize())

async def kill_worker(q):
    await q.join()
    await push(q, None)

async def add_node(entity, g):
    parent = entity['child_of']
    child = entity['pid']
    ids = [id['id'] for id in entity['ids'] if id['type'] == 'uid']
    if len(ids) != 0:
        uid = ids
    else:
        uid = ''
    g.node(child, '{}\n{}\n{}\n{}'.format(entity['title'], child, entity['type'],uid), shape=MAP[entity['type']]['shape'])
    if parent != 'tleo':
        g.edge(parent, child)

def main():
    max_workers = 5
    g = graphviz.Digraph(format='svg')
    q = asyncio.Queue()
    with aiohttp.ClientSession(connector=conn) as session:
        loop = asyncio.get_event_loop()
        init_task = loop.create_task(push(q, start_url))
        worker_tasks = [loop.create_task(worker(session, q, g)) for i in range(max_workers)]
        kill_tasks = [loop.create_task(kill_worker(q)) for i in range(max_workers)]
        loop.run_until_complete(asyncio.wait([init_task] + worker_tasks + kill_tasks))
    g.render(os.environ.get('HOME') + '/Tmp/' + pid + suffix + '.gv')

# parsing pips responses
def get_list(entity, key):
    return [i for i in entity[2:] if i[0] == key]

def format_id(id):
    data = {}
    data['id'] = id[2]
    data['type'] = id[1]['type']
    data['authority'] = id[1]['authority']
    return data

def parse_entity(entity):
    data = {}
    data['pid'] = entity[1]['pid']
    data['type'] = entity[0]
    data['ids'] = [format_id(i) for i in get_list(entity, 'ids')[0][2:] if isinstance(i, list)]
    data['links'] = [entity[1]['href'] + link + '?format=json' for link in MAP[entity[0]]['urls']]
    parent = get_list(entity, MAP[entity[0]]['rel_type'])
    if len(parent) == 0:
        data['child_of'] = 'tleo'
    else:
        data['child_of'] = [i for i in parent if isinstance(i, list)][0][2][1]['pid']
    if entity[0] in ('brand', 'series', 'episode', 'clip'):
        data['title'] = [i for i in get_list(entity, 'title') if isinstance(i, list)][0][2]
    else:
        data['title'] = ''
    return data

def format_next_page(next_page):
    data = {}
    data['type'] = 'page'
    data['links'] = [i['href'] for i in next_page[1:] if i['rel'] == 'pips-meta:pager-next']
    return data

def parse_response(response):
    if (isinstance(response[0], list) and response[0][0] == 'results'):
        total = int(response[0][1]['total'])
        if total == 0:
            return []
        elif total > 0:
            return [parse_entity(e) for e in response[0][2:]]
    elif (isinstance(response[0], list) and response[0][0] == 'links'):
        next_page = format_next_page(response[0][2])
        entities = [parse_entity(e) for e in response[1][2:]]
        entities.append(next_page)
        return entities
    else:
        return [parse_entity(response)]

if __name__ == '__main__':
    main()
