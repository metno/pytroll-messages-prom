#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2016,2019 Trygve Aspenes

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

import signal
import threading
import datetime as dt
import logging
import logging.handlers
# import os.path
try:
    import queue
except ImportError:
    import Queue as queue
import time
# from collections import OrderedDict
from six.moves.configparser import NoOptionError, RawConfigParser

# from posttroll import message, publisher
from posttroll.listener import ListenerContainer
# from trollsift import Parser, compose
import os
# import Process
import json
import posttroll.message

import mysql.connector
from mysql.connector import errorcode


class MessageHandler(object):

    """Listen for all messages and process them.
    """

    def __init__(self, config, section, queue):
        self._config = config
        self._section = section
        self._queue = queue
        topics = config.get(section, 'topics').split()

        signal.signal(signal.SIGINT, self.signal_stop)
        signal.signal(signal.SIGTERM, self.signal_stop)

        try:
            nameserver = config.get(section, 'nameserver')
            # nameserver = nameservers.split(',')
        except (NoOptionError, ValueError):
            nameserver = 'localhost'

        try:
            self.providing_server = config.get(section, 'providing-server').split(" ")
        except Exception:
            self.providing_server = None

        self._listener = ListenerContainer(topics=topics, nameserver=nameserver)
        # self._parser = Parser(self._pattern)

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
            except queue.Empty:
                continue

            #if msg.type == "file":
            if self.providing_server and msg.host not in self.providing_server:
                continue

            self.logger.info("New message received: %s", str(msg))
            self.process(msg)

    def signal_stop(self, signum, frame):
        self.logger.info("SIGNAL stop signum %s and frame %s", str(signum), str(frame))
        self.stop()

    def stop(self):
        """Stop MessageHandler."""
        self.logger.info("Stopping MessageHandler.")
        self._loop = False
        if self._listener is not None:
            self.logger.info("Before listener stop")
            self._listener.stop()
            self.logger.info("After listener stop")

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


def reset_skip_hosts(skip_hosts):
    try:
        skip_hosts.clear()
    except AttributeError:
        skip_hosts.pop()

def read_from_queue(queue, logger, hosts, data_points_before_write):
    # read from queue
    orig_hosts = list(hosts)
    skip_hosts = []
    message_data = {}
    statement = "insert into messages (topic, datetime, msg_host, type, jdoc) values (%s, %s, %s, %s, %s)"
    # set empty list for each hosts
    for host in orig_hosts:
        message_data[host] = []
    while True:
        logger.debug("Start waiting for new message in queue with queue size: {}".format(queue.qsize()))
        msg = queue.get()
        logger.info("Got new message. Queue size is now: {}".format(queue.qsize()))
        # if queue.qsize() == 0 and hosts != orig_hosts:
        #    hosts = list(orig_hosts)
        #    logger.info("Resetting hosts %s", str(hosts))
        logger.debug("Data   : {}".format(msg.data))
        logger.debug("Subject: {}".format(msg.subject))
        logger.debug("Type   : {}".format(msg.type))
        logger.debug("Sender : {}".format(msg.sender))
        logger.debug("Time   : {}".format(msg.time))
        logger.debug("Binary : {}".format(msg.binary))
        logger.debug("Version: {}".format(msg.version))

        if msg.type != "beat" and msg.type != 'ack':
            # print "Version: " + str(mysql.connector.__version__)
            message_data_point = (msg.subject, msg.time, msg.host, msg.type, json.dumps(msg.data, default=posttroll.message.datetime_encoder))
            logger.debug(message_data_point)
            for host in hosts:
                message_data[host].append(message_data_point)
                if len(message_data[host]) > data_points_before_write:
                    if host in skip_hosts:
                        logger.info("Skipping host %s for now.", str(host))
                        continue
                    logger.debug("%s %s", host, str(message_data[host]))
                    logger.info("Need to push data to db %s ...", str(host))
                    try:
                        cnx = mysql.connector.connect(user='polarsat', password='lilla land',
                                                      host=host,
                                                      database='pytrollmessages',
                                                      connection_timeout=10)

                    except mysql.connector.Error as err:
                        skip_hosts.append(host)
                        # After 10 minutes reset the skip_hosts list
                        t = threading.Timer(10 * 60, reset_skip_hosts, args=(skip_hosts, ))
                        t.start()

                        # hosts.remove(host)
                        # logger.info("Hosts is now: %s after removing %s", str(hosts), host)
                        logger.info("Skip hosts is now: %s", str(skip_hosts))
                        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                            logger.error("Something is wrong with your user name or password")
                        elif err.errno == errorcode.ER_BAD_DB_ERROR:
                            logger.error("Database does not exist")
                        else:
                            logger.error(err)
                    else:
                        message_insert = cnx.cursor(dictionary=True)
                        try:
                            # statement = "insert into messages (topic, datetime, msg_host, type, jdoc) value(\"{}\",\"{}\",\"{}\",\"{}\",'{}')".format(msg.subject, msg.time, msg.host, msg.type, json.dumps(msg.data, default=posttroll.message.datetime_encoder))
                            message_insert.executemany(statement, message_data[host])
                            cnx.commit()
                            logger.info("Inserted %d into host %s", message_insert.rowcount, host)
                            message_data[host] = []
                        except mysql.connector.Error as err:
                            logger.error("Failed insert message: {}".format(err))
                        finally:
                            message_insert.close()
                            cnx.close()
                else:
                    logger.info("Wait for more messages before writing to db. %s Got %d of %d.", str(host), len(message_data[host]), data_points_before_write)

        # logger.debug("{}".format())

        # print "READ FROM QUEUE:",msg

def write_to_queue(msg, meta, queue):
    # Write to queue
    # print "WRITE TO QUEUE"
    # print "Before write",queue.qsize()
    # msg.data['db_database'] = meta['db_database']
    # msg.data['db_passwd'] = meta['db_passwd']
    # msg.data['db_user'] = meta['db_user']
    # msg.data['db_host'] = meta['db_host']
    queue.put(msg)
    # print "After write",queue.qsize()


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

    print("Setting timezone to UTC")
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

    try:
        logger.info("config item: %s", args.config_item)
        logger.info("db_hosts %s ", config.get(args.config_item, 'db_hosts'))
        db_hosts = config.get(args.config_item, 'db_hosts').split(",")
    except Exception:
        logger.error("Failed to read db_hosts from config. use default")
        db_hosts = ['157.249.169.223']
    queue = Queue()

    data_points_before_write = config.getint(args.config_item, 'data_points_before_write')

    queue_handler = Process(target=read_from_queue, args=(queue, logger, db_hosts, data_points_before_write,))
    queue_handler.daemon = True
    queue_handler.start()
    logger.info("queue handler pid %s", str(queue_handler.pid))

    message_handler = MessageHandler(config, args.config_item, queue)
    message_handler.set_logger(logger)
    message_handler.run()

    logger.info("After message_handler.run()")
    queue_handler.terminate()
    logger.info("After queue_handler.terminate()")


if __name__ == "__main__":
    main()
