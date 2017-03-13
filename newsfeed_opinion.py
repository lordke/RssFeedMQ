#!/bin/env python
import os
import sys
import optparse
from threading import Thread
import time
from datetime import datetime, timedelta
from dateutil import tz
import psycopg2
import psycopg2.extras
import logging
import simplejson
from pprint import pprint, pformat
from collections import Counter
from pika.adapters import SelectConnection
try:
    import tracebackturbo as traceback
except:
    import traceback

import settings
from misc import *


LOGGER = logging.getLogger(__name__)
JSON_DECODER = simplejson.JSONDecoder()
JSON_ENCODER = simplejson.JSONEncoder()
NEG_WORDS = []
DB_CONN = None


def connect_pg():
    global DB_CONN
    try:
        DB_CONN = psycopg2.connect(settings.pg_db_dsn)
    except:
        LOGGER.info(sys.exc_info())
        quit()


class ProcessOpinion():
    def __init__(self):
        self.feed = {}
   
    def process(self, data):
        """ Detects opinion in a post.
        """
        LOGGER.info('Processing post data with post_id: %d' % (data['post_id'],))
        global DB_CONN
        try:
            if DB_CONN is None:
                connect_pg()
            cursor = DB_CONN.cursor()
            for track_id in data['track_id_list']:
                cursor.execute("INSERT INTO feedjack_trackstate " \
                               "(track_id, post_id, sentiment) " \
                               "VALUES (%s, %s, %s)",
                               (track_id, data['post_id'], 'neg'))
                DB_CONN.commit()
        except:
            DB_CONN.rollback()
            LOGGER.info(sys.exc_info())
        LOGGER.info('Processing post data with post_id: %d compelte' % (data['post_id'],))
    

class NewsFeedOpinion(Thread):
    def __init__(self):
        super(NewsFeedOpinion, self).__init__()
        self.feeds  = []
        self.update_sentimentwords()
        self.channel = None
        self.connection = None
        self.processopinion = ProcessOpinion()
        self.alive = False
        self.stop_flag = False
        self.r = None
        self.r_last_touched = datetime.now()
        
    def update_sentimentwords(self):
        global NEG_WORDS
        global DB_CONN
        try:
            LOGGER.info("Obtaining sentiment word list from postgre")
            if DB_CONN is None:
                connect_pg()
            cursor = DB_CONN.cursor()
            cursor.execute("SELECT name FROM sentidict_sentimentword WHERE mode='2'")
            results = cursor.fetchall()
            for result in results:
                NEG_WORDS.append(str(result[0]))
            LOGGER.info("Obtaining sentiment word list from postgre completed")
        except:
            DB_CONN.rollback()
            logger(sys.exc_info())  

    def post_init(self):
        try:
            LOGGER.info('Opening a connection')       
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
        
    def declare_exchange(self):
        LOGGER.info('Declaring an exchange')
        self.channel.exchange_declare(exchange=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_NAME,
                        exchange_type=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_TYPE,
                        passive=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_PASSIVE,
                        durable=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_DURABLE,
                        auto_delete=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_AUTO_DELETE,
                        internal=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_INTERNAL,
                        nowait=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_NOWAIT,
                        arguments=None, # Custom key/value pair arguments for the exchange
                        callback=self.on_exchange_declared) # Call this method on Exchange.DeclareOk
                        
    def on_exchange_declared(self, unused_frame):
        LOGGER.info('Declaring an exchange completed')
        self.declare_queue()

    def declare_queue(self):
        LOGGER.info('Declaring a queue')
        self.channel.queue_declare(callback=self.on_queue_declared,
                        queue=settings.RABBITMQ_NEWSFEED_ENTRY_QUEUE_NAME,
                        passive=settings.RABBITMQ_NEWSFEED_ENTRY_QUEUE_PASSIVE, 
                        durable=settings.RABBITMQ_NEWSFEED_ENTRY_QUEUE_DURABLE, 
                        exclusive=settings.RABBITMQ_NEWSFEED_ENTRY_QUEUE_EXCLUSIVE,
                        auto_delete=settings.RABBITMQ_NEWSFEED_ENTRY_QUEUE_AUTO_DELETE,
                        nowait=settings.RABBITMQ_NEWSFEED_ENTRY_QUEUE_NOWAIT,
                        arguments=None)

    def on_queue_declared(self, method_frame):
        LOGGER.info('Declaring a queue completed')
        self.bind_queue()
        
    def bind_queue(self):
        LOGGER.info('Binding a queue')
        self.channel.queue_bind(callback=self.on_queue_binded, 
                                queue=settings.RABBITMQ_NEWSFEED_ENTRY_QUEUE_NAME,
                                exchange=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_NAME,
                                routing_key=settings.RABBITMQ_NEWSFEED_ENTRY_ROUTING_KEY)
    
    def on_queue_binded(self, frame):
        LOGGER.info('Binding a queue completed')
        # Start the thread's activity
        self.start()
    
    def on_message(self, channel, basic_deliver, properties, body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

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
            self.processopinion.process(data)
        except simplejson.JSONDecodeError:
            LOGGER.info(sys.exc_info())
        
    def ack_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame
        """
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self.channel.basic_ack(delivery_tag)

    def run(self):
        LOGGER.info('Start to consume message from queue')
        self.channel.basic_consume(consumer_callback=self.on_message,
                                   queue=settings.RABBITMQ_NEWSFEED_ENTRY_QUEUE_NAME,
                                   no_ack=False, # Set to True means tell the broker to not expect a response
                                   exclusive=False, # Set to True means don't allow other consumers on the queue
                                   consumer_tag=None) # Specify your own consumer tag


class NewsFeedOpinionMgr(Thread):
    def __init__(self):
        super(NewsFeedOpinionMgr, self).__init__()
        self.maggot = None

    def get_newsfeed_opinion(self):
        LOGGER.info("Starting a NewsFeedOpinion...")
        maggot = NewsFeedOpinion()
        maggot.post_init()
        maggot.start()
        return maggot

    def run(self):
        while True:
            try:
                if self.maggot is None:
                    LOGGER.info("The NewsFeedOpinion is None. Create a new one.")
                    self.maggot = self.get_newsfeed_opinion()
            except KeyboardInterrupt:
                LOGGER.info("KeyboardInterrupt. So quitting. Bye!")
                quit()
            except Exception, e:
                LOGGER.info(traceback.format_exc())
                continue

def main():
    logging.basicConfig(level=logging.INFO, format=settings.LOG_FORMAT)
    mgr = NewsFeedOpinionMgr()
    mgr.start()

if __name__ == "__main__":
    main()



