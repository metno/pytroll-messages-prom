#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2022 Trygve Aspenes

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

"""Receive all messages in the system and make key information
available as metrics in an endpoint.
"""

from typing import Collection
from posttroll.subscriber import Subscribe
import yaml
from threading import Thread
import logging
import logging.handlers
# import os.path
try:
    import queue
except ImportError:
    import Queue as queue
import time

# from posttroll import message, publisher
from posttroll.listener import ListenerContainer
# from trollsift import Parser, compose
import os
# import Process
import json
import posttroll.message

from prometheus_client import start_http_server, Counter, Gauge

MESSAGE_START_TIME = Gauge('posttroll_message_start_time_seconds',
                           'Start time of the data usually parsed from the filename',
                           ['message_type', 'topic'])
MESSAGE_END_TIME = Gauge('posttroll_message_end_time_seconds',
                           'End time of the data usually parsed from the filename',
                           ['message_type', 'topic'])
MESSAGE_REGISTER_TIME = Gauge('posttroll_message_register_time_seconds',
                              'When the last message was registered at the end point',
                              ['message_type', 'topic'])
MESSAGE_NUMBER_OF_FILES = Gauge('posttroll_message_number_of_files_count',
                                'number of files in file, dataset or collection',
                                ['message_type', 'topic'])
MESSAGE_NUMBER_OF = Counter('posttroll_message_counter',
                            'Number of messages of this type since endpoint restart',
                            ['message_type', 'topic'])

class Listener(Thread):

    def __init__(self, queue, config, logger):
        Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.config = config
        #self.subscribe_nameserver = subscribe_nameserver
        self.logger = logger

    def stop(self):
        """Stops the file listener"""
        self.logger.debug("Entering stop in FileListener ...")
        self.loop = False
        self.queue.put(None)

    def run(self):
        print("HER")
        self.logger.debug("Entering run in FileListener ...")
        if type(self.config["subscribe-topic"]) not in (tuple, list, set):
            self.config["subscribe-topic"] = [self.config["subscribe-topic"]]
        try:
            if 'services' not in self.config:
                self.config['services'] = ''
            subscriber_addresses = None
            if 'subscriber_addresses' in self.config:
                subscriber_addresses = self.config['subscriber_addresses'].split(',')

            with Subscribe(self.config['services'], self.config['subscribe-topic'],
                           True, addresses=subscriber_addresses,
                           nameserver=self.config['nameserver']) as subscr:

                self.logger.debug("Entering for loop subscr.recv")
                for msg in subscr.recv(timeout=1):
                    if not self.loop:
                        # self.logger.debug("Self.loop false in FileListener {}".format(self.loop))
                        break

                    # self.logger.debug("Before checking message.")
                    # Check if it is a relevant message:
                    if self.check_message(msg):
                        self.logger.info("Put the message on the queue...")
                        self.logger.debug("Message = " + str(msg))
                        self.queue.put(msg)
                        self.logger.debug("After queue put.")
                    # else:
                    #     self.logger.warning("check_message returned False for some reason. Message is: %s", str(msg))

        except KeyError as ke:
            self.logger.info("Some key error. probably in config:", ke)
            raise

    def check_message(self, msg):

        if not msg:
            return False
        return True


def read_from_queue(queue, logger):
    # read from queue
    while True:
        try:
            logger.debug("Start waiting for new message in queue with queue size: {}".format(queue.qsize()))
            msg = queue.get()
            logger.info("Got new message. Queue size is now: {}".format(queue.qsize()))
            logger.debug("%s", str(msg))
            if msg.type != "beat" and msg.type != 'ack' and msg.type != 'info':
                logger.debug("Data   : {}".format(msg.data))
                logger.debug("Subject: {}".format(msg.subject))
                logger.debug("Type   : {}".format(msg.type))
                logger.debug("Sender : {}".format(msg.sender))
                logger.debug("Time   : {}".format(msg.time))
                logger.debug("Binary : {}".format(msg.binary))
                logger.debug("Version: {}".format(msg.version))
                try:
                    msg_host = msg.host.split(".")[0]
                except Exception:
                    msg_host = msg.host
                message_data_point = (msg.subject, msg.time, msg_host, msg.type, json.dumps(msg.data, default=posttroll.message.datetime_encoder))
                logger.debug(message_data_point)
                #dt = json.dumps(msg.data, default=posttroll.message.datetime_encoder)
                start_time = msg.data['start_time'].timestamp()
                try:
                    end_time = msg.data['start_time'].timestamp()
                except Exception:
                    end_time = start_time
                try:
                    number_of_files = 1
                    if isinstance(msg.data['uri'], list):
                        number_of_files = len(msg.data['uri'])
                except KeyError:
                    try:
                        number_of_files = len(msg.data['dataset'])
                    except KeyError:
                        try:
                            number_of_files = len(msg.data['collection'])
                        except KeyError:
                            number_of_files = 0

                MESSAGE_START_TIME.labels(message_type=msg.type, topic=msg.subject).set(start_time)
                MESSAGE_END_TIME.labels(message_type=msg.type, topic=msg.subject).set(end_time)
                MESSAGE_REGISTER_TIME.labels(message_type=msg.type, topic=msg.subject).set_to_current_time()
                MESSAGE_NUMBER_OF_FILES.labels(message_type=msg.type, topic=msg.subject).set(number_of_files)
                MESSAGE_NUMBER_OF.labels(message_type=msg.type, topic=msg.subject).inc()
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt. Shutting down.")
            break

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
    parser.add_argument("-c", "--config-file", help="config file to be used")
    parser.add_argument("-C", "--config_item", help="config item to use")
    parser.add_argument("-r", "--subscribe-nameserver",
                        type=str,
                        dest='subscribe_nameserver',
                        default="localhost",
                        help="subscribe nameserver, defaults to localhost")

    return parser.parse_args()

# Config management
def read_config(filename, debug=True):
    """Read the config file called *filename*.
    """
    with open(filename, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
            if debug:
                import pprint
                pp = pprint.PrettyPrinter(indent=4)
                pp.pprint(config)
        except FileNotFoundError:
            print("Could not find you config file:", filename)
            raise
        except yaml.YAMLError as exc:
            print("Failed reading yaml config file: {} with: {}".format(filename, exc))
            raise yaml.YAMLError

    return config

def main():
    '''Main. Parse cmdline, read config etc.'''

    from multiprocessing import Process, Queue

    args = arg_parse()

    # Create a metric from message key start_time
    start_http_server(8000)

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

    listener_queue = queue.Queue()

    config = None
    if os.path.exists(args.config_file):
        config = read_config(args.config_file, debug=False)

    listener = Listener(listener_queue, config, logger)
    listener.start()
    read_from_queue(listener_queue, logger)

    logger.info("After message_handler.run()")
    listener.terminate()
    logger.info("After queue_handler.terminate()")


if __name__ == "__main__":
    main()
