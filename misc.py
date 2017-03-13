import os
import sys
import re
import socket
from random import uniform
import time
from datetime import datetime, timedelta
from dateutil import tz
import psycopg2
import psycopg2.extras
from pprint import pformat, pprint
#import nltk
import hashlib
import csv, codecs, cStringIO
import rfc822
import redis
import simplejson

import settings

__author__ = 'gli'

db_conn = None







def encode(tstr):
    """ Encodes a unicode string in utf-8
    """
    if not tstr:
        return ''
    # this is _not_ pretty, but it works
    try:
        return tstr.encode('utf-8', "xmlcharrefreplace")
    except UnicodeDecodeError:
        # it's already UTF8.. sigh
        return tstr.decode('utf-8').encode('utf-8')


def prints(tstr):
    """ lovely unicode
    """
    sys.stdout.write('%s\n' % (tstr.encode(sys.getdefaultencoding(), 'replace')))
    sys.stdout.flush()


def mtime(time):
    """ Datetime auxiliar function.
        时区切换
    """
    # http://stackoverflow.com/questions/4770297/python-convert-utc-datetime-string-to-local-datetime
    # Auto-detect zones:
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()
    utc = datetime.strptime(time, "%a, %d %b %Y %H:%M:%S GMT")
    # Tell the datetime object that it's in UTC time zone since 
    # datetime objects are 'naive' by default
    utc = utc.replace(tzinfo=from_zone)
    # Convert time zone
    central = utc.astimezone(to_zone)
    return central


def logger(message):
    print "%s: %s [%s]" % (datetime.now(), message, sys.argv[0])


def logger_count(count):
    logger("Processed %s" % count)


def connect_pg():
    global db_conn
    try:
        db_conn = psycopg2.connect(settings.pg_db_dsn)
    except:
        logger(sys.exc_info())
        quit()
