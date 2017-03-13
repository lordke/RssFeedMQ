#!/bin/env python
import os
import sys
import optparse
import time
from datetime import datetime, timedelta
from dateutil import tz
import logging
import simplejson
from pprint import pprint, pformat
from collections import Counter
from pika.adapters import TornadoConnection
try:
    import tracebackturbo as traceback
except:
    import traceback
import tornado.ioloop
import tornado.web
import tornado.websocket
from tornado.websocket import WebSocketHandler

import settings
from misc import *


LOGGER = logging.getLogger(__name__)

JSON_DECODER = simplejson.JSONDecoder()
JSON_ENCODER = simplejson.JSONEncoder()



class PikaClient():
    def __init__(self, ioloop):
        self.ioloop = ioloop
        self.connected = False
        self.connecting = False
        self.connection = None
        self.channel = None
 
        self.event_listeners = set([])
 
    def connect(self):
        if self.connecting:
            LOGGER.info('Already connected to RabbitMQ server')
            return
 
        LOGGER.info('Opening a connection to RabbitMQ server')       
        self.connecting = True
     
        self.connection = TornadoConnection(parameters=settings.pika_parameters,
                                            on_open_callback=self.on_connection_open)
        self.connection.add_on_close_callback(self.on_closed)
        
        
    def on_connection_open(self, connection):
        LOGGER.info('Opening a connection to RabbitMQ server completed')
        self.connected = True
        self.connection = connection
        self.open_channel()
        
    def open_channel(self):
        LOGGER.info('Opening a channel')
        self.connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        LOGGER.info('Opening a channel completed')
        self.channel = channel
        self.declare_exchange()
         
#    def on_channel_open(self, channel):
#        LOGGER.info('PikaClient: Channel open, Declaring exchange')
#        self.channel = channel
#        # declare exchanges, which in turn, declare
#        # queues, and bind exchange to queues

    def declare_exchange(self):
        LOGGER.info('Declaring an exchange')
        self.channel.exchange_declare(exchange=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_NAME,
                        exchange_type=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_TYPE,
                        passive=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_PASSIVE,
                        durable=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_DURABLE,
                        auto_delete=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_AUTO_DELETE,
                        internal=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_INTERNAL,
                        nowait=settings.RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_NOWAIT,
                        arguments=None,
                        callback=self.on_exchange_declared)
                        
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
        self.channel.basic_consume(consumer_callback=self.on_message,
                                   queue=settings.RABBITMQ_NEWSFEED_ENTRY_QUEUE_NAME,
                                   no_ack=False, # Set to True means tell the broker to not expect a response
                                   exclusive=False, # Set to True means don't allow other consumers on the queue
                                   consumer_tag=None) # Specify your own consumer tag
        
 
    def on_closed(self, connection):
        LOGGER.info('Connection to RabbitMQ server closed')
        self.ioloop.stop()
 
    #def on_message(self, channel, method, header, body):
    def on_message(self, channel, basic_deliver, properties, body):
        LOGGER.info('Received message # %s from %s',
                    basic_deliver.delivery_tag, properties.app_id)
        self.ack_message(basic_deliver.delivery_tag)
        self.notify_listeners(body)
 
    def ack_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame
        """
        LOGGER.info('Acknowledging message %s', delivery_tag)
        self.channel.basic_ack(delivery_tag)
        
    def notify_listeners(self, event_obj):
        # here we assume the message the sourcing app
        # post to the message queue is in JSON format
        #event_json = json.dumps(event_obj)
        event_json = event_obj
 
        for listener in self.event_listeners:
            listener.write_message(event_json)
            LOGGER.info('Notified %s' % repr(listener))
 
    def add_event_listener(self, listener):
        self.event_listeners.add(listener)
        LOGGER.info('Added listener %s' % repr(listener))
 
    def remove_event_listener(self, listener):
        try:
            self.event_listeners.remove(listener)
            LOGGER.info('Removed listener %s' % repr(listener))
        except KeyError:
            pass
        
        
class NewsFeedWebSocketHandler(WebSocketHandler):
 
    def open(self, *args, **kwargs):
        self.application.pc.add_event_listener(self)
        LOGGER.info("WebSocket opened")
 
    def on_close(self):
        LOGGER.info("WebSocket closed")
        self.application.pc.remove_event_listener(self)


application = tornado.web.Application([
    (r'/ws', NewsFeedWebSocketHandler),
])


def main():
    logging.basicConfig(level=logging.INFO, format=settings.LOG_FORMAT)
 
    ioloop = tornado.ioloop.IOLoop.instance()
 
    # PikaClient is our rabbitmq consumer
    application.pc = PikaClient(ioloop)
    application.pc.connect()
 
    application.listen(settings.WEB_SOCKET_SERVER_PORT)
    ioloop.start()
    

if __name__ == "__main__":
    main()



