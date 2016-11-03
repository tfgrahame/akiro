#!/usr/bin/env python3

import aiohttp
import asyncio
import graphviz
import os
import ssl
import sys

#b0729l56
pid = sys.argv[1]
start_url = os.environ.get('PIPS_BASE') + 'pid.' + pid + '?format=json'

MAP = {'brand':{'rel_type':None, 'urls':['children/series', 'children/episodes', 'children/clips']},
       'series':{'rel_type':'member_of', 'urls':['children/series', 'children/episodes', 'children/clips']},
       'episode':{'rel_type':'member_of', 'urls':['children/versions', 'children/clips']},
       'clip':{'rel_type':'clip_of', 'urls':['versions']},
       'version':{'rel_type':'version_of', 'urls':['ondemands','media_assets','broadcasts']},
       'broadcast':{'rel_type':'broadcast_of', 'urls':[]},
       'media_asset':{'rel_type':'media_asset_of', 'urls':[]},
       'ondemand':{'rel_type':'broadcast_of', 'urls':[]}}

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
                    await add_node(entity['child_of'], entity['pid'], g)
                    for link in entity['links']:
                        await push(q, link)
                elif entity['type'] == 'page':
                    for link in entity['links']:
                        await push(q, link)
                else:
                    print('mad error')
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
    data['child_of'] = [i for i in get_list(entity, MAP[entity[0]]['rel_type']) if isinstance(i, list)][0][2][1]['pid']
    data['ids'] = [format_id(i) for i in get_list(entity, 'ids')[0][2:] if isinstance(i, list)]
    data['links'] = [entity[1]['href'] + link + '?format=json' for link in MAP[entity[0]]['urls']]
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
