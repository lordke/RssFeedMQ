import pika
import socket
HOSTNAME = socket.gethostname()

# For "feedparser" package
VERSION = '0.9.16'
URL = 'http://www.newsfeed.org/'
USER_AGENT = 'newsfeed %s - %s' % (VERSION, URL)


# Program's general settings
DEBUG = False
# Should I be verbose?
DEFAULT_VERBOSE = False
# Suspend time in seconds when connecting to feeds
DEFAULT_SLEEPTIME = 30
# Wait timeout in seconds when connecting to feeds
DEFAULT_SOCKETTIMEOUT = 10
# Number of worker threads that will fetch feeds in parallel
DEFAULT_WORKERTHREADS = 10
# This is a slow feed if the downloading time exceeds this limit
SLOWFEED_WARNING = 10
# Log format
LOG_FORMAT = '%(asctime)s - %(levelname)s: %(name)s: %(message)s'


# Postgresql database settings
pg_db_host = "localhost"
pg_db_port = 5432
pg_db_dbname = "rssdata"
pg_db_username = "joe"
pg_db_password = "password"
pg_db_dsn = "host=%(host)s port=%(port)s dbname=%(dbname)s user=%(username)s password=%(password)s" % {
    "host": pg_db_host,
    "port": pg_db_port,
    "dbname": pg_db_dbname,
    "username": pg_db_username,
    "password": pg_db_password,
    }


# Rabbitmq settings
RABBITMQ_SERVER_IP = "localhost"
RABBITMQ_SERVER_PORT = 5672
RABBITMQ_SERVER_USERNAME = "guest"
RABBITMQ_SERVER_PASSWORD = "guest"
pika_parameters = pika.ConnectionParameters(host=RABBITMQ_SERVER_IP,
                                            port=RABBITMQ_SERVER_PORT,
                                            credentials=pika.PlainCredentials(
                                            RABBITMQ_SERVER_USERNAME, 
                                            RABBITMQ_SERVER_PASSWORD))

RABBITMQ_NEWSFEED_RAW_FEED_EXCHANGE_NAME = "yuqing.newsfeed.exchange.raw_feed" # The exchange name to use
RABBITMQ_NEWSFEED_RAW_FEED_EXCHANGE_TYPE = "direct" # The exchange type to use
RABBITMQ_NEWSFEED_RAW_FEED_EXCHANGE_PASSIVE = False # Perform a declare or just check to see if it exists
RABBITMQ_NEWSFEED_RAW_FEED_EXCHANGE_DURABLE = False # Survive or die out during a reboot of RabbitMQ 
RABBITMQ_NEWSFEED_RAW_FEED_EXCHANGE_AUTO_DELETE = False # Remove or keep when no more queues are bound to it
RABBITMQ_NEWSFEED_RAW_FEED_EXCHANGE_INTERNAL = False # Can only be published to by other exchanges?
RABBITMQ_NEWSFEED_RAW_FEED_EXCHANGE_NOWAIT = False # Do not expect an Exchange.DeclareOk response
RABBITMQ_NEWSFEED_RAW_FEED_QUEUE_NAME = "yuqing.newsfeed.queue.raw_feed" # The queue name
RABBITMQ_NEWSFEED_RAW_FEED_QUEUE_PASSIVE = False # Only check to see if the queue exists
RABBITMQ_NEWSFEED_RAW_FEED_QUEUE_DURABLE = False # Survive reboots of the broker
RABBITMQ_NEWSFEED_RAW_FEED_QUEUE_EXCLUSIVE = False # Only allow access by the current connection?
RABBITMQ_NEWSFEED_RAW_FEED_QUEUE_AUTO_DELETE = False # Delete after consumer cancels or disconnects
RABBITMQ_NEWSFEED_RAW_FEED_QUEUE_NOWAIT = False # Do not wait for a Queue.DeclareOk
RABBITMQ_NEWSFEED_RAW_FEED_ROUTING_KEY = "yuqing.newsfeed.routing_key.raw_feed.__normal__"

RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_NAME = "yuqing.newsfeed.exchange.entry" 
RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_TYPE = "fanout"
RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_PASSIVE = False
RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_DURABLE = False
RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_AUTO_DELETE = False
RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_INTERNAL = False
RABBITMQ_NEWSFEED_ENTRY_EXCHANGE_NOWAIT = False
RABBITMQ_NEWSFEED_ENTRY_QUEUE_NAME = "yuqing.newsfeed.queue.entry"
RABBITMQ_NEWSFEED_ENTRY_QUEUE_PASSIVE = False
RABBITMQ_NEWSFEED_ENTRY_QUEUE_DURABLE = False
RABBITMQ_NEWSFEED_ENTRY_QUEUE_EXCLUSIVE = False
RABBITMQ_NEWSFEED_ENTRY_QUEUE_AUTO_DELETE = False
RABBITMQ_NEWSFEED_ENTRY_QUEUE_NOWAIT = False
RABBITMQ_NEWSFEED_ENTRY_ROUTING_KEY = ""


# WebSocket settings
WEB_SOCKET_SERVER_PORT = 8888
