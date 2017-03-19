#!/bin/env python
#coding=utf-8
import os
import sys
import socket
import optparse
from threading import Thread
import time
from datetime import datetime, timedelta
#from dateutil import tz
import psycopg2
import psycopg2.extras
from pprint import pprint, pformat
from collections import Counter
import logging
import simplejson
from pika.adapters import SelectConnection
try:
    import tracebackturbo as traceback
except:
    import traceback

import settings
from misc import *


ENTRY_NEW, ENTRY_UPDATED, ENTRY_SAME, ENTRY_ERR = range(4)


JSON_DECODER = simplejson.JSONDecoder()
JSON_ENCODER = simplejson.JSONEncoder()

LOGGER = logging.getLogger(__name__)

TRACK_DICT = {}

DB_CONN = None

def connect_pg():
    global DB_CONN
    try:
        DB_CONN = psycopg2.connect(settings.pg_db_dsn)
    except:
        LOGGER.info(sys.exc_info())
        quit()


class ProcessEntry:
    """Processes entries
    
    The "entries" is a list of dictionaries. Each dictionary contains data from a different entry. 
    Entries are listed in the order in which they appear in the original feed.
    """
    def __init__(self, feed, entry, post_dict):
        self.feed = feed
        self.entry = entry
        self.post_dict = post_dict
            
    def get_tags(self):
        """ Returns a list of tag objects from an entry.
            返回tag对象的列表
        """
        tags = []
        if 'tags' in self.entry:
            for tag in self.entry['tags']:
                if tag['label'] != None:
                    term = tag['label']
                else:
                    term = tag['term']
                #去除空格，分割tags，得到tag list，然后存储到feedjack_tag表中返回 tags（id，tagname）list，id为数据库表中的id
                qcat = term.strip()
                if ',' in qcat or '/' in qcat:
                    qcat = qcat.replace(',', '/').split('/')
                else:
                    qcat = [qcat]
                for zcat in qcat:
                    tagname = zcat.lower()
                    while '  ' in tagname:
                        tagname = tagname.replace('  ', ' ')
                    tagname = tagname.strip()
                    if not tagname or tagname == ' ':
                        continue
                          
                    try:
                        if DB_CONN is None:
                            connect_pg()
                        cursor = DB_CONN.cursor(cursor_factory=psycopg2.extras.DictCursor)
                        cursor.execute("SELECT id FROM feedjack_tag " \
                                       "WHERE name=%s", 
                                       (tagname,))
                        results = cursor.fetchall()
                        if results:
                            tags.append({'id': results[0]['id'],
                                         'name': tagname})
                        else:
                            cursor.execute("INSERT INTO feedjack_tag (name) " \
                                           "VALUES (%s)",
                                           (tagname,))
                            DB_CONN.commit()
                            cursor.execute("SELECT id FROM feedjack_tag " \
                                           "WHERE name=%s",
                                           (tagname,))
                            results = cursor.fetchall()
                            if results:
                                tags.append({'id': results[0]['id'],
                                             'name': tagname})                  
                    except:
                        DB_CONN.rollback()
                        LOGGER.info(sys.exc_info())
        return tags

    def get_entry_data(self):
        """ Retrieves data from a post and returns it in a tuple.
            取出post（entry）中的数据并返回tuple，包括 link, title, guid , author_detail(name, email),creater,content(summary,description),modified_parsed,commerts，tags等
        """
        #pprint(self.entry)
        try:
            link = self.entry['link']
        except AttributeError:
            link = self.feed['link']
        try:
            title = self.entry['title']
        except AttributeError:
            title = link
        guid = self.entry.get('id', title)

        if self.entry.has_key('author_detail'):
            author = self.entry['author_detail'].get('name', '')
            author_email = self.entry['author_detail'].get('email', '')
        else:
            author = ''
            author_email = ''

        if not author:
            author = self.entry.get('author', self.entry.get('creator', ''))
        if not author_email:
            # this should be optional~
            author_email = 'nospam@nospam.com'
        
        try:
            content = self.entry['content'][0]['value']
        except:
            content = self.entry.get('summary',
                                     self.entry.get('description', ''))
        
        if 'modified_parsed' in self.entry:
            date_modified = mtime(self.entry['modified_parsed'])
        else:
            date_modified = None

        tags = self.get_tags()
        comments = self.entry.get('comments', '')

        return (link, title, guid, author, author_email, content, 
                date_modified, tags, comments)
            
    def process(self):
        """ Process a post in a feed and saves it in the database if necessary.
            处理单个报文，并在需要时存储到 数据库中
        """
        (link, title, guid, author, author_email, content, date_modified, 
         fcat, comments) = self.get_entry_data()
        
        hitted_track_ids = self.can_save_post(title, content) #关键词命中
        if hitted_track_ids == []:
            LOGGER.info('This post has no interesting stuff. So discard it.')
            return None, None

        LOGGER.debug(u'Entry\n' \
                   u'  title: %s\n' \
                   u'  link: %s\n' \
                   u'  guid: %s\n' \
                   u'  author: %s\n' \
                   u'  author_email: %s\n' \
                   u'  tags: %s' % (
                title, link, guid, author, author_email,
                u' '.join(tag['name'] for tag in fcat)))

        if guid in self.post_dict: #entry与现有entry重复
            print '!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!111guid in self.post_dict'
            tobj = self.post_dict['guid']
            if tobj.content != content or (date_modified and
                    tobj.date_modified != date_modified):
                retval = ENTRY_UPDATED #标记为 文章更新状态
                if self.options.verbose:
                    prints('[%d] Updating existing post: %s' % (
                           self.feed.id, link))
                if not date_modified:
                    # damn non-standard feeds
                    date_modified = tobj.date_modified
                tobj.title = title
                tobj.link = link
                tobj.content = content
                tobj.guid = guid
                tobj.date_modified = date_modified
                tobj.author = author
                tobj.author_email = author_email
                tobj.comments = comments
                tobj.tags.clear()
                [tobj.tags.add(tcat) for tcat in fcat]
                #tobj.save()
                ############################################
                #publish_to_redis(title, link, content, guid, date_modified, author)
                #detect_monitored_item2(tobj)
                
                ############################################
            else:
                retval = ENTRY_SAME #文章未更新状态
                LOGGER.info('[Feed id: %d] Post has not changed: %s' % (self.feed.id, link))
            post = None
        else:
            # execute this
            retval = ENTRY_NEW #新文章，存储到数据库
            # TEMPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP
#            if not date_modified and self.fpf:
#                # if the feed has no date_modified info, we use the feed
#                # mtime or the current time
#                if self.fpf.feed.has_key('modified_parsed'):
#                    date_modified = mtime(self.fpf.feed.modified_parsed)
#                elif self.fpf.has_key('modified'):
#                    date_modified = mtime(self.fpf.modified)
            # TEMPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP
            if not date_modified:
                date_modified = datetime.now()
 
            LOGGER.info('[Feed id: %d] Saving new post: %s' % (self.feed['id'], link))            
            global DB_CONN
            try:
                if DB_CONN is None:
                    connect_pg()
                # Insert data to "feedjack_post"
                cursor = DB_CONN.cursor(cursor_factory=psycopg2.extras.DictCursor)       
                cursor.execute("INSERT INTO feedjack_post " \
                               "(feed_id, title, link, content, guid, " \
                               "author, author_email, comments, date_modified) " \
                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                               (self.feed['id'], title, link, content, guid, author, 
                                author_email, comments, date_modified))
                DB_CONN.commit()
                
                # Get the id of the latest inserted record
                post_id = None
                cursor.execute("SELECT id FROM feedjack_post WHERE "
                               "feed_id=%s AND title=%s AND link=%s AND " \
                               "content=%s AND guid=%s AND author=%s AND " \
                               "author_email=%s AND comments=%s AND date_modified=%s",
                               (self.feed['id'], title, link, 
                                content, guid, author, 
                                author_email, comments, date_modified))         
                results = cursor.fetchall()
                if results:
                    post_id = results[0]['id']
                
                # Insert data to "feedjack_post_tags" table, which represents
                # a Django ManyToMany relationship between "feedjack_post" and
                # "feedjack_tag".
                for tcat in fcat:
                    # Get or create a "feedjack_tag" record according to tcat
                    #获取或创建feedjack_tag 并创造eedjack_post_tags表
                    tag_id = None
                    tag_name = tcat['name']
                    cursor.execute("SELECT id FROM feedjack_tag WHERE name=%s", (tag_name,))
                    results = cursor.fetchall()
                    if results:
                        tag_id = results[0]['id']
                    else:
                        cursor.execute("INSERT INTO feedjack_tag " \
                                       "(name) " \
                                       "VALUES (%s)",
                                       (tag_name,))
                        DB_CONN.commit()
                        cursor.execute("SELECT id FROM feedjack_tag " \
                                       "WHERE name=%s",
                                       (tag_name,))
                        results = cursor.fetchall()
                        if results:
                            tag_id = results[0]['id']
                    # Insert data to "feedjack_post_tags" table
                    if post_id and tag_id:
                        cursor.execute("INSERT INTO feedjack_post_tags " \
                                       "(post_id, tag_id) " \
                                       "VALUES (%s, %s)",
                                       (post_id, tag_id))
                        DB_CONN.commit()
            except:
                DB_CONN.rollback()
                LOGGER.info(sys.exc_info())
                
            post = {'post_id': post_id,
                    'feed_id': self.feed['id'],
                    'track_id_list': hitted_track_ids,
                    'title': title,
                    'link': link,
                    'content': content,
                    'guid': guid,
                    'author': author,
                    'author': author_email,
                    'comments': comments,
                    'date_modified': date_modified.strftime("%Y-%m-%d %H:%M:%S")}
        return retval, post
    
    def can_save_post(self, title, content):
        #关键词TRACK_DICT（track_id, trackedphrases），检查其中的title和content是否符合，并返回track_id
        title = title.encode('utf-8')
        content = content.encode('utf-8')
        hitted_track_ids = []
        for track_id, trackedphrases in TRACK_DICT.iteritems():
            hitted = 0
            for trackedphrase in trackedphrases:
                terms = trackedphrase['name'].split(' ')
                for term in terms:
                    if term in title or term in content:
                        hitted += 1
                        break    
            if hitted == len(trackedphrases):
                hitted_track_ids.append(track_id)      
        return hitted_track_ids


class ProcessFeed():
    def __init__(self):
        self.feed = {}
        # data --> feed
    def process(self, data):
        """ Parses a feed data.
            解析feed数据的 id ,etag, last_modified ,title,tagline, link, last_checke,并存储到数据库里
        """
        LOGGER.info('Processing feed data with feed_id: %d' % (data['feed_id'],))

        self.feed['id'] = data['feed_id']
        # If the feed has changed (or it is the first time we parse it),
        # save the etag and last_modified fields
        self.feed['etag'] = data.get('etag', '')
        # Sometimes this is None (it never should) *sigh*
        if self.feed['etag'] is None:
            self.feed['etag'] = ''

        # This "last_modified" field is returned from the remote feed server
        try:
            self.feed['last_modified'] = mtime(data['modified'])
        except:
            #pass
            self.feed['last_modified'] = data['last_modified']
        
        self.feed['title'] = data['feed'].get('title', '')[0:254]
        self.feed['tagline'] = data['feed'].get('tagline', '')
        self.feed['link'] = data['feed'].get('link', '')
        self.feed['last_checked'] = datetime.now()

#        LOGGER.info(u'[%d] Feed info for: %s\n' \
#                    u'  title %s\n' \
#                    u'  tagline %s\n' \
#                    u'  link %s\n' \
#                    u'  last_checked %s' % (
#                    self.feed['id'], self.feed['feed_url'], self.feed['title'],
#                    self.feed['tagline'], self.feed['link'], self.feed['last_checked']))

        # Update "feedjack_feed" table in database
        LOGGER.info('Updating "feedjack_feed" table in database')
        try:
            if DB_CONN is None:
                connect_pg()
            cursor = DB_CONN.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute("UPDATE feedjack_feed SET " \
                           "title=%s, tagline=%s, link=%s, etag=%s, " \
                           "last_modified=%s, last_checked=%s " \
                           "WHERE id=%s",
                           (self.feed['title'], self.feed['tagline'], 
                            self.feed['link'], self.feed['etag'], 
                            self.feed['last_modified'], self.feed['last_checked'], 
                            self.feed['id']))        
            DB_CONN.commit()                       
        except:
            DB_CONN.rollback()
            LOGGER.info(sys.exc_info())
        LOGGER.info('Updating "feedjack_feed" table in database completed')
        
        # Get guids
        guids = []
        for entry in data['entries']: # rss feed中的不同 条目
            if entry.get('id', ''):
                guids.append(entry.get('id', ''))
            elif 'title' in entry:
                guids.append(entry['title'])
            elif 'link' in entry:
                guids.append(entry['link'])
        
        existing_posts = {}
        if guids:
            #existing_posts = dict([(post.guid, post) for post in models.Post.objects.filter(feed=self.feed.id).filter(guid__in=guids)])
            try:
                if DB_CONN is None:
                    connect_pg()
                cursor = DB_CONN.cursor(cursor_factory=psycopg2.extras.DictCursor)

                #查找相同feed.id 的entries的中 guid相同的entry并保存到results中，并取出放到existing_posts dict中

                cursor.execute("SELECT id, feed_id, title, link, content, guid, " \
                               "author, author_email, comments, date_modified " \
                               "FROM feedjack_post " \
                               "WHERE feed_id=%s AND guid IN %s",
                               (self.feed['id'], tuple(guids)))
                results = cursor.fetchall()
                for result in results:
                    existing_posts[result['guid']] = {'id': result['id'],
                                                'feed_id': result['feed_id'],
                                                'title': result['title'],
                                                'link': result['link'],
                                                'content': result['content'],
                                                'guid': result['guid'],
                                                'author': result['author'],
                                                'author_email': result['author_email'],
                                                'comments': result['comments'],
                                                'date_modified': result['date_modified']}
            except:
                DB_CONN.rollback()
                LOGGER.info(sys.exc_info())
                
        entries_status = {ENTRY_NEW: 0,
                          ENTRY_UPDATED: 0,
                          ENTRY_SAME: 0,
                          ENTRY_ERR: 0}
        new_post_list = [] #处理完成的所有post
        for entry in data['entries']:
            try:
                (entry_status, post) = self.process_entry(entry, existing_posts)
                #if entry_status and post:
                if post:
                    new_post_list.append(post)
                    entries_status[entry_status] += 1
            except:
                (etype, eobj, etb) = sys.exc_info()
                print '[%d] ! -------------------------' % (self.feed['id'],)
                print traceback.format_exception(etype, eobj, etb)
                traceback.print_exception(etype, eobj, etb)
                print '[%d] ! -------------------------' % (self.feed['id'],)
                entry_status = ENTRY_ERR
          
        return entries_status, new_post_list

    
    def process_entry(self, entry, existing_posts):
        """Wrapper for ProcessEntry
        封装 ProcessEntry类的process
        """
        pentry = ProcessEntry(self.feed, entry, existing_posts)
        (entry_status, post) = pentry.process()
        del pentry
        return entry_status, post
        
        

class NewsFeedMaggot(Thread):
    def __init__(self):
        super(NewsFeedMaggot, self).__init__()
        self.feeds  = []
        self.update_track_dict()
        self.processfeed = ProcessFeed()
        self.channel = None
        self.connection = None
        self.alive = False
        self.stop_flag = False
        self.r = None
        self.r_last_touched = datetime.now()
            
    def update_track_dict(self):
        global DB_CONN
        try:
            LOGGER.info("Obtaining track from postgre")
            if DB_CONN is None:
                connect_pg()
            # Get all ids from "feedjack_track"
            track_ids = []
            cursor = DB_CONN.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute("SELECT id FROM feedjack_track")
            results = cursor.fetchall()
            for result in results:
                track_ids.append(result['id'])
            # Get "trackedphrase" for each "track"
            for track_id in track_ids:
                TRACK_DICT[track_id] = []
                cursor.execute("SELECT id, name, mode " \
                               "FROM feedjack_trackedphrase " \
                               "WHERE track_id=%s", (track_id,))
                results = cursor.fetchall()
                for result in results:
                    TRACK_DICT[track_id].append({'trackedphrase_id': result['id'],
                                                 'name': result['name'],
                                                 'mode': result['mode']})       
            LOGGER.info("Obtaining trackedphrase completed")
            #pprint(TRACK_DICT)
        except:
            DB_CONN.rollback()
            LOGGER.info(sys.exc_info())

    def post_init(self):
        #初始化 新建RabbitMQ 队列和exchange并绑定（2个）
        LOGGER.info('Initializing a FeedProcessor')
        #self.feedprocessor.post_init()
        LOGGER.info('Initializing a FeedProcessor completed')
        try:
            LOGGER.info('Opening a connection')
            #self.dispatcher = Dispatcher(self.options)        
            self.connection = SelectConnection(parameters=settings.pika_parameters,
                                on_open_callback=self.on_connection_open)
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
        
    def open_channel(self):
        LOGGER.info('Opening a channel')
        self.connection.channel(on_open_callback=self.on_channel_open)
    
    def on_channel_open(self, new_channel):
        LOGGER.info('Opening a channel completed')
        self.channel = new_channel
        self.declare_exchange()
        self.declare_exchange2()
        
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
                        
    def on_exchange_declared(self, unused_frame):
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
        # Start the thread's activity
        self.start()
    ###############################   
    def declare_exchange2(self):
        LOGGER.info('Declaring an exchange')
        self.channel.exchange_declare(exchange=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_NAME,
                        exchange_type=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_TYPE,
                        passive=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_PASSIVE,
                        durable=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_DURABLE,
                        auto_delete=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_AUTO_DELETE,
                        internal=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_INTERNAL,
                        nowait=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_NOWAIT,
                        arguments=None, # Custom key/value pair arguments for the exchange
                        callback=self.on_exchange_declared2) # Call this method on Exchange.DeclareOk

    def on_exchange_declared2(self, frame):
        LOGGER.info('Declaring an exchange completed')
        self.declare_queue2()

    def declare_queue2(self):
        LOGGER.info('Declaring a queue')
        self.channel.queue_declare(callback=self.on_queue_declared2,
                           queue=settings.RABBITMQ_NEWSFEED_ENTRY_QUEUE_NAME,
                           passive=settings.RABBITMQ_NEWSFEED_ENTRY_QUEUE_PASSIVE, 
                           durable=settings.RABBITMQ_NEWSFEED_ENTRY_QUEUE_DURABLE, 
                           exclusive=settings.RABBITMQ_NEWSFEED_ENTRY_QUEUE_EXCLUSIVE,
                           auto_delete=settings.RABBITMQ_NEWSFEED_ENTRY_QUEUE_AUTO_DELETE,
                           nowait=settings.RABBITMQ_NEWSFEED_ENTRY_QUEUE_NOWAIT,
                           arguments=None)

    def on_queue_declared2(self, method_frame):
        LOGGER.info('Declaring a queue completed')
        self.bind_queue2()
        
    def bind_queue2(self):
        LOGGER.info('Binding a queue')
        self.channel.queue_bind(callback=self.on_queue_binded2, 
                                queue=settings.RABBITMQ_NEWSFEED_ENTRY_QUEUE_NAME,
                                exchange=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_NAME,
                                routing_key=settings.RABBITMQ_NEWSFEED_ENTRY_ROUTING_KEY)
        
    def on_queue_binded2(self, frame):
        LOGGER.info('Binding a queue completed on 2')
        
    def on_message(self, channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent

        收到消息message，processfeed 类处理成单个postlist并发送给websocket

        :param pika.channel.Channel channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body
        """
        LOGGER.info('Received message # %s from %s',
                    basic_deliver.delivery_tag, properties.app_id)
        self.ack_message(basic_deliver.delivery_tag)
        
        try:
            data = JSON_DECODER.decode(body)
            #pprint(data)
            (entries_status, post_list) = self.processfeed.process(data)
            for post in post_list:
                LOGGER.info('Publishing data (post id: %d) to %s', 
                            post['post_id'],
                            settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_NAME)
                json_data = JSON_ENCODER.encode(post)
                self.channel.basic_publish(exchange=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_NAME,
                                           routing_key=settings.RABBITMQ_NEWSFEED_ENTRY_ROUTING_KEY,
                                           body=json_data)
                LOGGER.info('Publishing data completed')

        except simplejson.JSONDecodeError:
            LOGGER.info(sys.exc_info())
        #except simplejson.JSONEncodeError:
        #    LOGGER.info(sys.exc_info())
            
    def ack_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame
        """
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self.channel.basic_ack(delivery_tag)

    def run(self):
        #设置channel.basic_consume MQ消息接收
        LOGGER.info('Start to consume message from queue')
        self.channel.basic_consume(consumer_callback=self.on_message,
                                   queue=settings.RABBITMQ_NEWSFEED_RAW_FEED_QUEUE_NAME,
                                   no_ack=False, # Set to True means tell the broker to not expect a response
                                   exclusive=False, # Set to True means don't allow other consumers on the queue
                                   consumer_tag=None) # Specify your own consumer tag


class NewsFeedMaggotMgr(Thread):
    #用于生成maggot（NewsfeedMaggot）类的实例
    def __init__(self):
        super(NewsFeedMaggotMgr, self).__init__()
        self.maggot = None

    def get_newsfeed_maggot(self):
        LOGGER.info("Starting a maggot...")
        maggot = NewsFeedMaggot()
        maggot.post_init()
        maggot.start()
        return maggot

    def run(self):
        while True:
            try:
                if self.maggot is None:
                    LOGGER.info("The maggot is None. Create a new one.")
                    self.maggot = self.get_newsfeed_maggot()
            except KeyboardInterrupt:
                LOGGER.info("KeyboardInterrupt. So quitting. Bye!")
                quit()
            except Exception, e:
                LOGGER.info(traceback.format_exc())
                continue

def main():
    logging.basicConfig(level=logging.INFO, format=settings.LOG_FORMAT)
    mgr = NewsFeedMaggotMgr()
    #newsfeedmaggot管理类
    mgr.start()

if __name__ == "__main__":
    main()



