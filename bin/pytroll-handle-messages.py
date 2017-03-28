#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016 Trygve Aspenes

# Author(s): Trygve Aspenes

#   Trygve Aspenes <trygveas@met.no>

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Receive all messages in the system and insert them into a db
"""

import datetime as dt
import logging
import logging.handlers
#import os.path
import Queue
import time
#from collections import OrderedDict
from ConfigParser import NoOptionError, RawConfigParser

#from posttroll import message, publisher
from posttroll.listener import ListenerContainer
#from trollsift import Parser, compose
import os
#import Process
import json
import posttroll.message

class MessageHandler(object):

    """Listen for all messages and process them.
    """

    def __init__(self, config, section, queue):
        self._config = config
        self._section = section
        self._queue = queue
        topics = config.get(section, 'topics').split()

        try:
            nameservers = config.get(section, 'nameservers')
            nameservers = nameservers.split(',')
        except (NoOptionError, ValueError):
            nameservers = []

        self._listener = ListenerContainer(topics=topics)
        #self._parser = Parser(self._pattern)

    def set_logger(self, logger):
        """Set logger."""
        self.logger = logger

    def run(self):
        """Run MessageHandler"""
        self._loop = True
        while self._loop:
            # Check listener for new messages
            msg = None
            try:
                msg = self._listener.output_queue.get(True, 1)
            except AttributeError:
                msg = self._listener.queue.get(True, 1)
            except KeyboardInterrupt:
                self.stop()
                continue
            except Queue.Empty:
                continue

            #if msg.type == "file":
            self.logger.info("New message received: %s", str(msg))
            self.process(msg)

    def stop(self):
        """Stop MessageHandler."""
        self.logger.info("Stopping MessageHandler.")
        self._loop = False
        if self._listener is not None:
            self._listener.stop()

    def process(self, msg):
        """Process message"""
        try:
            #self.logger.debug("Message: {}".format(msg))
            meta = dict()
            write_to_queue(msg, meta, self._queue)
            #mda = self._parser.parse(msg.data["uid"])
        except ValueError:
            self.logger.debug("Unknown file, skipping.")
            return

def read_from_queue(queue, logger):
    #read from queue
    while True:
        logger.debug("Start waiting for new message in queue qith queue size: {}".format(queue.qsize()))
        msg = queue.get()
        logger.debug("Got new message. Queue size is now: {}".format(queue.qsize()))
        logger.debug("Data   : {}".format(msg.data))
        logger.debug("Subject: {}".format(msg.subject))
        logger.debug("Type   : {}".format(msg.type))
        logger.debug("Sender : {}".format(msg.sender))
        logger.debug("Time   : {}".format(msg.time))
        logger.debug("Binary : {}".format(msg.binary))
        logger.debug("Version: {}".format(msg.version))

        if msg.type != "beat":
            import mysql.connector
            from mysql.connector import errorcode
            #print "Version: " + str(mysql.connector.__version__)

            try:
                cnx = mysql.connector.connect(user='polarsat', password='lilla land',
                                              host='10.99.3.111',
                                              database='pytrollmessages')
                
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    print("Something is wrong with your user name or password")
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    print("Database does not exist")
                else:
                    print(err)
            else:
                message_insert = cnx.cursor(dictionary=True)
                try:
                    statement = "insert into t1 (topic, datetime, type, jdoc) value(\"{}\",\"{}\",\"{}\",'{}')".format(msg.subject, msg.time, msg.type, json.dumps(msg.data, default=posttroll.message.datetime_encoder))
                    exed = message_insert.execute(statement)
                    cnx.commit()
                except mysql.connector.Error as err:
                    print("Failed insert message: {}".format(err))
                finally:
                    message_insert.close()

        #logger.debug("{}".format())

        #print "READ FROM QUEUE:",msg

def write_to_queue(msg, meta, queue):
    #Write to queue
    #print "WRITE TO QUEUE"
    #print "Before write",queue.qsize()
    #msg.data['db_database'] = meta['db_database']
    #msg.data['db_passwd'] = meta['db_passwd']
    #msg.data['db_user'] = meta['db_user']
    #msg.data['db_host'] = meta['db_host']
    queue.put(msg)
    #print "After write",queue.qsize()

def arg_parse():
    '''Handle input arguments.
    '''
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log",
                        help="File to log to (defaults to stdout)",
                        default=None)
    parser.add_argument("-v", "--verbose", help="print debug messages too",
                        action="store_true")
    parser.add_argument("-c", "--config", help="config file to be used")
    parser.add_argument("-C", "--config_item", help="config item to use")

    return parser.parse_args()


def main():
    '''Main. Parse cmdline, read config etc.'''

    from multiprocessing import Process, Queue

    args = arg_parse()

    config = RawConfigParser()
    config.read(args.config)

    print "Setting timezone to UTC"
    os.environ["TZ"] = "UTC"
    time.tzset()

    handlers = []
    if args.log:
        handlers.append(
            logging.handlers.TimedRotatingFileHandler(args.log,
                                                      "midnight",
                                                      backupCount=7))

    handlers.append(logging.StreamHandler())

    if args.verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO
    for handler in handlers:
        handler.setFormatter(logging.Formatter("[%(levelname)s: %(asctime)s :"
                                               " %(name)s] %(message)s",
                                               '%Y-%m-%d %H:%M:%S'))
        handler.setLevel(loglevel)
        logging.getLogger('').setLevel(loglevel)
        logging.getLogger('').addHandler(handler)

    logging.getLogger("posttroll").setLevel(logging.INFO)
    logger = logging.getLogger("MessageHandler")

    queue=Queue()

    queue_handler = Process(target=read_from_queue, args=(queue,logger,))
    queue_handler.daemon=True
    queue_handler.start()

    message_handler = MessageHandler(config, args.config_item, queue)
    message_handler.set_logger(logger)
    message_handler.run()


if __name__ == "__main__":
    main()

