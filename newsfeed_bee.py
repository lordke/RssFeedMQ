#!/bin/env python
#coding=utf-8
import os
import sys
import socket
import urllib
import urllib2
import optparse
import psycopg2
import psycopg2.extras
import feedparser
import re
import simplejson
import logging
from threading import Thread
from time import mktime, sleep
from datetime import datetime, timedelta
from dateutil import tz
from pprint import pprint, pformat
from collections import Counter
from pika.adapters import SelectConnection
try:
    import tracebackturbo as traceback
except:
    import traceback
import settings
from misc import logger, logger_count
from misc import *


FEED_OK, FEED_UNCHANGED, FEED_ERRPARSE, FEED_ERRHTTP, FEED_ERREXC = range(5)

JSON_DECODER = simplejson.JSONDecoder()
JSON_ENCODER = simplejson.JSONEncoder()

LOGGER = logging.getLogger(__name__)


threadpool = None # always make it None for testing
db_conn = None



# Register a third-party date handler to feedparser
# See http://packages.python.org/feedparser/date-parsing.html
_my_date_pattern = re.compile(
    r'(\d{,2})/(\d{,2})/(\d{4}) (\d{,2}):(\d{2}):(\d{2})')

def my_date_handler(aDateString):
    """parse a UTC date in MM/DD/YYYY HH:MM:SS format"""
    month, day, year, hour, minute, second = _my_date_pattern.search(aDateString).groups()
    return (int(year), int(month), int(day), int(hour), int(minute), int(second), 0, 0, 0)

#feedparser.registerDateHandler(my_date_handler)


def mtime(time):
    """ Datetime auxiliar function.

    转换时区
    UTC =>当前时区
    """
    # http://stackoverflow.com/questions/4770297/python-convert-utc-datetime-string-to-local-datetime
    # Auto-detect zones:
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    utc = datetime.datetime.strptime(time, "%a, %d %b %Y %H:%M:%S GMT")
    
    
    #from time import mktime
    #from datetime import datetime

    #utc = datetime.fromtimestamp(mktime(time))

    # Tell the datetime object that it's in UTC time zone since 
    # datetime objects are 'naive' by default
    utc = utc.replace(tzinfo=from_zone)
    # Convert time zone
    central = utc.astimezone(to_zone)
    return central


def connect_pg():
    #连接数据库
    global db_conn
    try:
        db_conn = psycopg2.connect(settings.pg_db_dsn)
    except:
        LOGGER.info(sys.exc_info())
        quit()


class Dispatcher:
    """A job dispatcher."""
    def __init__(self, options):
        self.options = options
        self.feed_status_dict = {FEED_OK: 0,
                                 FEED_UNCHANGED: 0,
                                 FEED_ERRPARSE: 0,
                                 FEED_ERRHTTP: 0,
                                 FEED_ERREXC: 0}
        self.feed_trans = {FEED_OK: 'OK',
                           FEED_UNCHANGED: 'UNCHANGED',
                           FEED_ERRPARSE: 'CAN\'T PARSE',
                           FEED_ERRHTTP: 'HTTP ERROR',
                           FEED_ERREXC: 'EXCEPTION'}
        self.feed_keys = sorted(self.feed_trans.keys())
        if threadpool:
            self.tpool = threadpool.ThreadPool(self.options.workerthreads)
        else:
            self.tpool = None
        self.time_start = datetime.now()
        # Set socket timeout
        #socket.setdefaulttimeout(self.options.timeout)

    def add_job(self, feed):
        """ Adds a feed processing job to the pool
            添加RSS拉取任务
        """
        if self.tpool:
            req = threadpool.WorkRequest(self.process_feed, (feed,))
            return self.tpool.putRequest(req)
        else: # This will execute
            # No threadpool module, just run the job
            LOGGER.info(u'Processing feed [id:%d], [feed_url:%s]' % (feed['id'], feed['feed_url']))
            start_time = datetime.now()
            data, feed_status = self.process_feed(feed)
            #process_feed(）方法见下获取解析数据和状态
            delta = datetime.now() - start_time
            if delta.seconds > self.options.slowfeedtime:
                LOGGER.info(u' (SLOW FEED!)')
                
            self.feed_status_dict[feed_status] += 1

            LOGGER.info('Feed status is ' + self.feed_trans[feed_status])
            return data

    def process_feed(self, feed):
        """Downloads feed using feedparser.
        
        It checks the ETag and the Last-Modified time to save bandwith and avoid bans.
        检查E-tag和 Last-Modified时间 来节省带宽和防止被网站ban
        ETags and Last-Modified headers are two ways that feed publishers can save bandwidth, 
        but they only work if clients take advantage of them. The basic concept is that a feed 
        publisher may provide a special HTTP header, called an ETag, when it publishes a feed. 
        You should send this ETag back to the server on subsequent requests. If the feed has 
        not changed since the last time you requested it, the server will return a special 
        HTTP status code (304) and no feed data. Clients should support both ETag and 
        Last-Modified headers, as some servers support one but not the other.
        """
        try:
            data = feedparser.parse(feed['feed_url'],
                                    agent=settings.USER_AGENT,
                                    etag=feed['etag'],
                                    modified=feed['last_modified'])
        except:
            LOGGER.info('ERROR: feed cannot be parsed')
            '''
            返回的值为（type，value，traceback）。
            它们的含义是：type获取正在处理的异常的异常类型（类对象）;
            value获取异常参数（其关联值或第二个参数raise，如果异常类型是类对象，它总是一个类实例）;
            traceback获取一个跟踪对象（参见参考手册），它将调用堆栈封装在异常最初发生的点。
            '''
            (etype, eobj, etb) = sys.exc_info()
            print traceback.format_exception(etype, eobj, etb)
            traceback.print_exception(etype, eobj, etb)
            return None, FEED_ERRPARSE
        try:
            if hasattr(data, 'status'):
                LOGGER.info(u'HTTP status: %d' % (data.status,))
                if data.status == 304:
                    # If the feed has not changed since the last time we requested it, the 
                    # server will return a special HTTP status code (304) and no feed data.
                    LOGGER.info('Feed [id:%d] has not changed since last check' % (feed['id'],))
                    feed_status = FEED_UNCHANGED
                elif data.status >= 400:
                    # http error, ignore
                    LOGGER.info('Feed [id:%d] HTTP Error!' % (feed['id'],))
                    feed_status = FEED_ERRHTTP
                else:
                    feed_status = FEED_OK
            if hasattr(data, 'bozo') and data.bozo:
                feed_status = FEED_ERRHTTP ###!
                # The "bozo" is an integer, either 1 or 0. 
                # Set to 1 if the feed is not well-formed XML, and 0 otherwise.
                LOGGER.info('BOZO! Feed [id:%d] is not well formed' % (feed['id'],))
        except:
            (etype, eobj, etb) = sys.exc_info()
            print '[%d] ! -------------------------' % (feed['id'],)
            print traceback.format_exception(etype, eobj, etb)
            traceback.print_exception(etype, eobj, etb)
            print '[%d] ! -------------------------' % (feed['id'],)
            feed_status = FEED_ERREXC

        if feed_status == FEED_OK:
            return data, feed_status
        else:
            return None, feed_status


class NewsFeedBee(Thread):
    def __init__(self, options):
        super(NewsFeedBee, self).__init__()
        self.options = options
        self.dispatcher = None
        self.feeds  = []
        self.update_feeds()
        # RabbitMQ stuffs
        self.channel = None
        self.connection = None
        self.alive = False
        self.stop_flag = False

        #self.r = None
        #self.r_last_touched = datetime.now()
        
    def update_feeds(self):
        global db_conn
        try:
            LOGGER.info("Obtaining feeds to process from database")
            if db_conn is None:
                connect_pg()
            cursor = db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute("SELECT id, name, feed_url, etag, last_modified " \
                           "FROM feedjack_feed " \
                           "WHERE is_active=%s " 
                           "ORDER BY id", (True,))
            results = cursor.fetchall()
            for result in results:
                #if result['last_modified'] == None:
                #    last_modified = None
                #else:
                #    last_modified = str(result['last_modified'])
                self.feeds.append({'id': result['id'],
                                   'name': result['name'],
                                   'feed_url': str(result['feed_url']),
                                   'etag': str(result['etag']),
                                   'last_modified': result['last_modified']})
            LOGGER.info("Obtaining feeds to process from database completed")
        except:
            db_conn.rollback()
            LOGGER.info(sys.exc_info())
        
    def post_init(self):
        # 初始化分发任务类dispatcher
        # 通过异步的方式 打开rabbitMQ的连接，并新建channel，声明exchange，声明queue，绑定exchange和queue，然后运行run方法，之前运行ioloop
        self.dispatcher = Dispatcher(self.options)
        try:
            LOGGER.info('Opening a pika connection')
            self.connection = SelectConnection(parameters=settings.pika_parameters,
                                               on_open_callback=self.on_connection_open,
                                               on_open_error_callback=self.on_connection_open_error)
            try:
                LOGGER.info('Starting ioloop')
                self.connection.ioloop.start()
            except KeyboardInterrupt:
                # Gracefully close the connection
                self.connection.close()
                # Loop until we're fully closed, will stop on its own
                self.connection.ioloop.start()
        except:
            (etype, eobj, etb) = sys.exc_info()
            print traceback.format_exception(etype, eobj, etb)

    def on_connection_open(self, unused_connection):
        LOGGER.info('Opening a connection completed')
        self.open_channel()
        
    def on_connection_open_error(self, connection):
        LOGGER.info('Opening a connection failed')
        
    def open_channel(self):
        LOGGER.info('Opening a channel')
        self.connection.channel(on_open_callback=self.on_channel_open)
        
    def on_channel_open(self, new_channel):
        LOGGER.info('Opening a channel completed')
        self.channel = new_channel
        self.declare_exchange()
        
    def declare_exchange(self):
        LOGGER.info('Declaring an exchange')
        self.channel.exchange_declare(exchange=settings.RABBITMQ_NEWSFEED_RAW_FEED_EXCHANGE_NAME,
                        exchange_type=settings.RABBITMQ_NEWSFEED_RAW_FEED_EXCHANGE_TYPE,
                        passive=settings.RABBITMQ_NEWSFEED_RAW_FEED_EXCHANGE_PASSIVE,
                        durable=settings.RABBITMQ_NEWSFEED_RAW_FEED_EXCHANGE_DURABLE,
                        auto_delete=settings.RABBITMQ_NEWSFEED_RAW_FEED_EXCHANGE_AUTO_DELETE,
                        internal=settings.RABBITMQ_NEWSFEED_RAW_FEED_EXCHANGE_INTERNAL,
                        nowait=settings.RABBITMQ_NEWSFEED_RAW_FEED_EXCHANGE_NOWAIT,
                        arguments=None, # Custom key/value pair arguments for the exchange
                        callback=self.on_exchange_declared) # Call this method on Exchange.DeclareOk
        
    def on_exchange_declared(self, frame):
        LOGGER.info('Declaring an exchange completed')
        self.declare_queue()

    def declare_queue(self):
        LOGGER.info('Declaring a queue')
        self.channel.queue_declare(self.on_queue_declared,
                                   settings.RABBITMQ_NEWSFEED_RAW_FEED_QUEUE_NAME)

    def on_queue_declared(self, method_frame):
        LOGGER.info('Declaring a queue completed')
        self.bind_queue()
        
    def bind_queue(self):
        LOGGER.info('Binding a queue')
        self.channel.queue_bind(callback=self.on_queue_binded, 
                                queue=settings.RABBITMQ_NEWSFEED_RAW_FEED_QUEUE_NAME,
                                exchange=settings.RABBITMQ_NEWSFEED_RAW_FEED_EXCHANGE_NAME,
                                routing_key=settings.RABBITMQ_NEWSFEED_RAW_FEED_ROUTING_KEY)
        
    def on_queue_binded(self, frame):
        LOGGER.info('Binding a queue completed')
        self.start() # Start the thread's activity
    
    def run(self):
        while True:
            for feed in self.feeds:
                if not self.dispatcher:
                    self.dispatcher = Dispatcher(self.options)
                #if not self.channel:
                #    self.on_connected()
                #返回在dispatcher处理后传来的数据（即是feedparser抓取的数据）
                data = self.dispatcher.add_job(feed)
                if data is not None and isinstance(data, dict):
                    if settings.DEBUG:
                        filename = 'tmp/feed_' + str(feed['id']) + '_raw.txt'
                        with open(filename, 'w') as f:
                            f.write((pformat(data)).decode('utf-8'))

                    if hasattr(data, 'updated_parsed'):
                        del data['updated_parsed']
                                                    
                    if hasattr(data['feed'], 'published_parsed'):
                        #data['feed']['published'] = datetime.fromtimestamp(
                        #                            mktime(data['feed']['published_parsed']))
                        del data['feed']['published_parsed']
                    if hasattr(data['feed'], 'updated_parsed'):
                        #data['feed']['updated'] = datetime.fromtimestamp(
                        #                            mktime(data['feed']['updated_parsed']))
                        del data['feed']['updated_parsed']
      
                    # "data['entries']" is a list of dictionaries. 
                    # Each dictionary contains data from a different entry.
                    for idx, val in enumerate(data['entries']):
                        if hasattr(val, 'created_parsed'):
                            #data['entries'][idx]['created'] = datetime.fromtimestamp(
                            #                        mktime(val['created_parsed']))
                            del data['entries'][idx]['created_parsed']
                        if hasattr(val, 'expired_parsed'):
                            del data['entries'][idx]['expired_parsed']
                            
                        if hasattr(val, 'published_parsed'):
                            del data['entries'][idx]['published_parsed']
                            
                        if hasattr(val, 'updated_parsed'):
                            del data['entries'][idx]['updated_parsed']
                                                                         
                    data['feed_id'] = feed['id'] 
                    #data['last_modified'] = feed['last_modified']                      
                try:                
                    # If this throws an error, don't put message in queue
                    json_data = JSON_ENCODER.encode(data)
                    if settings.DEBUG:
                        filename = 'tmp/feed_' + str(feed['id']) + '.json'    
                        with open(filename, 'w') as f:
                            #simplejson.dump(json_data, f)
                            f.write(json_data) 
                    LOGGER.info('Publishing data to queue')
                    self.channel.basic_publish(exchange=settings.RABBITMQ_NEWSFEED_RAW_FEED_EXCHANGE_NAME, # The exchange to publish to
                                               routing_key=settings.RABBITMQ_NEWSFEED_RAW_FEED_ROUTING_KEY, # The routing key to bind on
                                               body=json_data) # The message body 
                    LOGGER.info('Publishing data to queue completed')
                except simplejson.JSONDecodeError:
                    LOGGER.info(sys.exc_info())
                except KeyboardInterrupt:
                    LOGGER.info("KeyboardInterrupt, so quitting! Bye!")
                    quit()
                except SystemExit:
                    quit()
                except:
                    LOGGER.info(sys.exc_info())
            
            # Suspend execution for the given number of seconds.
            LOGGER.info('Sleeping ' + str(self.options.sleeptime) + ' seconds. Zzz...')
            sleep(self.options.sleeptime)
            
    def stop(self):
        pass


class NewsFeedBeeMgr(Thread):
    def __init__(self, options):
        super(NewsFeedBeeMgr, self).__init__()
        self.options = options
        self.x = None

    def get_newsfeed_bee(self):
        LOGGER.info("Starting a NewsFeedBee")
        bee = NewsFeedBee(self.options)
        bee.post_init()
        # Start the thread's activity. It must be called at most once per thread object. 
        # It arranges for the object's run() method to be invoked in a separate thread 
        # of control.
        bee.start()
        return bee

    def need_to_refresh(self):
        if self.x is None:
            return False
#         new_data_dict = build_data_dict()
#         if compare_data_dicts(new_data_dict, self.x.data_dict) == False:
#             return False
        else:
            return True

    def run(self):
        while True:
            try:
                if self.x is None:
                    LOGGER.info("NewsFeedBee is None. Create a new one.")
                    self.x = self.get_newsfeed_bee()
            except KeyboardInterrupt:
                LOGGER.info("KeyboardInterrupt. So quitting. Bye!")
                quit()
            except Exception, e:
                LOGGER.info(traceback.format_exc())
                continue

def main():
    """Main funtion.
    
    This script connects to feed sites and fetches the content into rosebud_twitter_feed
    """
    parser = optparse.OptionParser(usage='%prog [options]', version=settings.USER_AGENT)
    parser.add_option('-f', '--feed', action='append', type='int', help='A feed id to be updated. This option can be given multiple times to update several feeds at the same time (-f 1 -f 4 -f 7).')
    parser.add_option('-s', '--site', type='int', help='A site id to update.')
    parser.add_option('-v', '--verbose', action='store_true', dest='verbose', default=settings.DEFAULT_VERBOSE, help='Verbose output.')
    parser.add_option('-l', '--sleeptime', type='int', default=settings.DEFAULT_SLEEPTIME, help='Suspend time in seconds when connecting to feeds.')
    parser.add_option('-t', '--timeout', type='int', default=settings.DEFAULT_SOCKETTIMEOUT, help='Wait timeout in seconds when connecting to feeds.')
    parser.add_option('-o', '--slowfeedtime', type='int', default=settings.SLOWFEED_WARNING, help='It is a slow feed if downloading time is exceeds this time in seconds.')
    parser.add_option('-w', '--workerthreads', type='int', default=settings.DEFAULT_WORKERTHREADS, help='Worker threads that will fetch feeds in parallel.')
    options = parser.parse_args()[0]
    logging.basicConfig(level=logging.INFO, format=settings.LOG_FORMAT)
    mgr = NewsFeedBeeMgr(options)
    mgr.start()

if __name__ == "__main__":
    main()
