#!/usr/bin/env python3

import logging
import re
import signal
import sys
import urllib.parse
import os.path
import sqlite3

import asyncio
import aiohttp
from lxml import html


class Crawler:

    db_file = 'db.sqlite3'
    conn = None
    cursor = None

    def __init__(self, rooturl, loop, maxtasks=100, maxlevel=1, url_xpath={}):
        self.rooturl = rooturl
        self.loop = loop
        self.todo = set()
        self.busy = set()
        self.done = {}
        self.tasks = set()
        self.sem = asyncio.Semaphore(maxtasks)
        self.maxlevel = maxlevel
        self.url_xpath = url_xpath

        # session stores cookies between requests and uses connection pool
        self.session = aiohttp.Session()

    def init_db(self):
        if not os.path.isfile(Crawler.db_file):
            Crawler.conn = sqlite3.connect(Crawler.db_file)
            Crawler.cursor = Crawler.conn.cursor()
            sql = """CREATE TABLE links (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                    page_url   TEXT,
                    page_title TEXT,
                    site_name  TEXT,
                    site_url   TEXT,
                    site_desc  TEXT);"""
            Crawler.cursor.execute(sql)
            Crawler.conn.commit()
        else:
            Crawler.conn = sqlite3.connect(Crawler.db_file)
            Crawler.cursor = Crawler.conn.cursor()

    @asyncio.coroutine
    def insert_db(self, dbvalue):
        Crawler.cursor.execute("INSERT INTO links VALUES (NULL,?,?,?,?,?);", dbvalue)

    @asyncio.coroutine
    def run(self):
        self.init_db()
        asyncio.Task(self.addurls([(self.rooturl, '')], 0))  # Set initial work.
        yield from asyncio.sleep(1)
        while self.busy:
            Crawler.conn.commit()
            yield from asyncio.sleep(1)

        self.session.close()
        self.loop.stop()
        Crawler.conn.commit()
        Crawler.cursor.close()
        Crawler.conn.close()


    @asyncio.coroutine
    def addurls(self, urls, currentlevel):
        for url, parenturl in urls:
            url = urllib.parse.urljoin(parenturl, url)
            url, frag = urllib.parse.urldefrag(url)
            if (url.startswith(self.rooturl) and
                    url not in self.busy and
                    url not in self.done and
                    url not in self.todo and
                    currentlevel <= self.maxlevel):
                self.todo.add(url)
                yield from self.sem.acquire()
                task = asyncio.Task(self.process(url, currentlevel))
                task.add_done_callback(lambda t: self.sem.release())
                task.add_done_callback(self.tasks.remove)
                self.tasks.add(task)

    @asyncio.coroutine
    def process(self, url, currentlevel):
        print('processing:', url)

        self.todo.remove(url)
        self.busy.add(url)
        try:
            resp = yield from aiohttp.request(
                'get', url, session=self.session)
        except Exception as exc:
            print('...', url, 'has error', repr(str(exc)))
            self.done[url] = False
        else:
            if resp.status == 200 and resp.get_content_type() == 'text/html':
                data = (yield from resp.read()).decode('utf-8', 'replace')
                element = html.fromstring(data)
                urls = []
                for urlregex in self.url_xpath:
                    regexp = re.compile(urlregex)
                    if regexp.match(url):
                        dbvalue = [url, None, None, None, None] # [page_url, page_title, site_name, site_url, site_desc]
                        for field in self.url_xpath[urlregex]['fields']:
                            value = element.find(self.url_xpath[urlregex]['fields'][field])
                            if value != None:
                                if value.tag == 'a':
                                    site_name = value.text
                                    site_url = value.attrib['href']
                                    dbvalue[2] = site_name
                                    dbvalue[3] = site_url
                                    print(field + ': '+ site_name + ', ' + site_url)
                                elif value.tag == 'title':
                                    page_title = value.text
                                    dbvalue[1] = page_title
                                    print(field + ': ' + page_title)
                                else:
                                    site_description = html.tostring(value, method='text').decode(encoding='UTF-8')
                                    if field == 'description':
                                        dbvalue[4] = site_description
                                    print(field + ': ' + site_description)
                        if dbvalue[2]:
                            dbvalue = tuple(dbvalue)
                            asyncio.Task(self.insert_db(dbvalue))
                        nextelements = element.xpath(self.url_xpath[urlregex]['xpath'])
                        urls = [elem.find('./a').attrib['href'] for elem in nextelements]
                currentlevel += 1
                asyncio.Task(self.addurls([(u, url) for u in urls], currentlevel))

            resp.close()
            self.done[url] = True

        self.busy.remove(url)
        print(len(self.done), 'completed tasks,', len(self.tasks),
              'still pending, todo', len(self.todo))


example_rooturl = 'http://www.dmoz.org/'

url_xpath_example = {
                     '^http[s]?:\/\/[^\/]+\/$': # 'regular expression of url'
                        {
                         'xpath': './/div[@class="one-third"][1]/span[1]', # 'XPath of links to be crawled next'
                         'fields': {
                                    'title': './/title', # content to scrape at the current url
                                   }
                        }, # top level
                     '^http[s]?:\/\/[^\/]+\/[^\/]+\/.*$':
                        {
                         'xpath': './/div[contains(concat(" ",@class," "), "dir-1 borN")]/ul/li',
                         'fields': {
                                    'title': './/title',
                                    'lastupdate': './/div[@class="ft-barUpN"]',
                                    'directoryurl': './/fieldset[@class="fieldcap"]/ul[@class="directory-url"]/li/a',
                                    'description': './/fieldset[@class="fieldcap"]/ul[@class="directory-url"]/li',
                                   }
                        }, # second level
                    }

def main():
    loop = asyncio.get_event_loop()

    try:
        rooturl = sys.argv[1]
    except IndexError:
        rooturl = example_rooturl
    
    maxlevel = 1
    try:
        maxlevel = int(sys.argv[2])
    except IndexError:
        pass
    
    c = Crawler(rooturl, loop, maxlevel=maxlevel, url_xpath=url_xpath_example)
    asyncio.Task(c.run())

    try:
        loop.add_signal_handler(signal.SIGINT, loop.stop)
    except RuntimeError:
        pass
    loop.run_forever()
    print('todo:', len(c.todo))
    print('busy:', len(c.busy))
    print('done:', len(c.done), '; ok:', sum(c.done.values()))
    print('tasks:', len(c.tasks))


if __name__ == '__main__':
    if '--iocp' in sys.argv:
        from asyncio import events, windows_events
        sys.argv.remove('--iocp')
        logging.info('using iocp')
        el = windows_events.ProactorEventLoop()
        events.set_event_loop(el)

    main()
