#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (c) 2022 

# Author(s):

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

import os
import sys
import time
import yaml
import atexit
import signal
import pickle
import logging
import logging.handlers
from threading import Thread
from datetime import datetime
from posttroll.subscriber import Subscribe
try:
    import queue
except ImportError:
    import Queue as queue

from prometheus_client import start_http_server, Counter, Gauge

MESSAGE_START_TIME = Gauge('posttroll_message_start_time_seconds',
                           'Start time of the data usually parsed from the filename',
                           ['message_type', 'topic', 'platform_name'])
MESSAGE_END_TIME = Gauge('posttroll_message_end_time_seconds',
                           'End time of the data usually parsed from the filename',
                           ['message_type', 'topic', 'platform_name'])
MESSAGE_REGISTER_TIME = Gauge('posttroll_message_register_time_seconds',
                              'When the last message was registered at the end point',
                              ['message_type', 'topic', 'platform_name'])
MESSAGE_NUMBER_OF_FILES = Gauge('posttroll_message_number_of_files_count',
                                'number of files in file, dataset or collection',
                                ['message_type', 'topic', 'platform_name'])
MESSAGE_NUMBER_OF = Counter('posttroll_message_counter',
                            'Number of messages of this type since endpoint restart',
                            ['message_type', 'topic', 'platform_name'])

sat_tr = {'Metop-B': 'metop-b',
          'METOPB': 'metop-b',
          'metopb': 'metop-b',
          'M01': 'metop-b',
          'Metop-C': 'metop-c',
          'METOPC': 'metop-c',
          'metopc': 'metop-c',
          'M03': 'metop-c',
          'NOAA-20': 'noaa 20',
          'NOAA 20': 'noaa 20',
          'noaa 20': 'noaa 20',
          'NOAA20': 'noaa 20',
          'noaa20': 'noaa 20',
          'N20': 'noaa 20',
          'NOAA-19': 'noaa 19',
          'NOAA 19': 'noaa 19',
          'noaa19': 'noaa 19',
          'NOAA-18': 'noaa 18',
          'NOAA 18': 'noaa 18',
          'Suomi-NPP': 'suomi npp',
          'NPP': 'suomi npp',
          'snpp': 'suomi npp',
          'EOS-Terra': 'terra',
          'TERRA': 'terra',
          'EOS-Aqua': 'aqua',
          'AQUA': 'aqua',
          'MOD': 'terra',
          'MYD': 'aqua',
          'FY3D': 'fengyun 3d',
          'FY-3D': 'fengyun 3d',
          'FY3C': 'fengyun 3c',
          'Fengyun-3D': 'fengyun 3d',
          'Fengyun-3E': 'fengyun 3e',
          'fy3d': 'fengyun 3d',
          'S3A': 'sentinel 3a',
          'S3B': 'sentinel 3b',
          'DK01': 'himawari 8',
          'Himawari-8': 'himawari 8',
          'G16': 'goes 16',
          'GOES-16': 'goes 16',
          'G17': 'goes 17',
          'GOES-17': 'goes 17',
          'G18': 'goes 18',
          'GOES-18': 'goes 18',
          'MSG1': 'meteosat 8',
          'MSG2': 'meteosat 9',
          'MSG3': 'meteosat 10',
          'MSG4': 'meteosat 11',
          'MET8': 'meteosat 8',
          'MET9': 'meteosat 9',
          'MET10': 'meteosat 10',
          'MET11': 'meteosat 11'}

class Listener(Thread):

    def __init__(self, queue, config, logger):
        Thread.__init__(self)
        self.loop = True
        self.queue = queue
        self.config = config
        self.logger = logger

    def stop(self):
        """Stops the file listener"""
        self.logger.debug("Entering stop in FileListener ...")
        self.loop = False
        self.queue.put(None)

    def run(self):
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
                # for msg in subscr.recv(timeout=1):
                for msg in subscr.recv():
                    if not self.loop:
                        self.logger.warning("Self.loop false in FileListener %s. Breaking.", str(self.loop))
                        break

                    if not msg:
                        continue

                    self.logger.info("Put the message on the queue...")
                    self.logger.info("Message = " + str(msg))
                    self.queue.put(msg)
                    self.logger.debug("After queue put.")

        except KeyError as ke:
            self.logger.info("Some key error. probably in config:", ke)
            raise

def _update_latest_status(msg, latest_status, platform_name, start_time, end_time, number_of_files):
    if msg.type not in latest_status:
        latest_status[msg.type] = {}
    if msg.subject not in latest_status[msg.type]:
        latest_status[msg.type][msg.subject] = {}
    if platform_name not in latest_status[msg.type][msg.subject]:
        latest_status[msg.type][msg.subject][platform_name] = {}
    latest_status[msg.type][msg.subject][platform_name]['start_time'] = start_time
    latest_status[msg.type][msg.subject][platform_name]['end_time'] = end_time
    latest_status[msg.type][msg.subject][platform_name]['current_time'] = datetime.now().timestamp()
    latest_status[msg.type][msg.subject][platform_name]['number_of_files'] = number_of_files
    return

def _init_from_startup_status(startup_status):
    for message_type in startup_status:
        for message_subject in startup_status[message_type]:
            for pn in startup_status[message_type][message_subject]:
                MESSAGE_START_TIME.labels(message_type=message_type, topic=message_subject, platform_name=pn).set(
                    startup_status[message_type][message_subject][pn]['start_time'])
                MESSAGE_END_TIME.labels(message_type=message_type, topic=message_subject, platform_name=pn).set(
                                        startup_status[message_type][message_subject][pn]['end_time'])
                MESSAGE_REGISTER_TIME.labels(message_type=message_type, topic=message_subject, platform_name=pn).set(
                                        startup_status[message_type][message_subject][pn]['current_time'])                    
                MESSAGE_NUMBER_OF_FILES.labels(message_type=message_type, topic=message_subject, platform_name=pn).set(
                                        startup_status[message_type][message_subject][pn]['number_of_files'])                    
                MESSAGE_NUMBER_OF.labels(message_type=message_type, topic=message_subject, platform_name=pn).inc()
    return

def read_from_queue(listener_queue, logger, startup_status, latest_status):
    # read from queue
    _init_from_startup_status(startup_status)                
    # Resetting latest status to avoid unwanted metrics
    while True:
        try:
            logger.debug("Start waiting for new message in queue with queue size: {}".format(listener_queue.qsize()))
            try:
                msg = listener_queue.get(timeout=60)
            except queue.Empty:
                continue
            logger.info("Got new message. Queue size is now: {}".format(listener_queue.qsize()))
            #logger.info("%s", str(msg))
            if msg.type != "beat" and msg.type != 'info':

                try:
                    start_time = msg.data['start_time'].timestamp()
                except KeyError:
                    try:
                        start_time = msg.data['nominal_time'].timestamp()
                    except KeyError:
                        logger.error(f"Failed to get start/nominal time from message: {str(msg.data)}")
                        continue
                except AttributeError:
                    try:
                        start_time = datetime.fromisoformat(msg.data['start_time']).timestamp()
                    except ValueError as ve:
                        logger.error(f"Failed to get start time from message: {str(msg.data)}\n{str(ve)}")
                        continue
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
                platform_name = "unknown"
                try:
                    platform_name = sat_tr.get(msg.data['platform_name'], msg.data['platform_name'])
                except KeyError:
                    if '0deg' in msg.subject:
                        platform_name = "meteosat 0deg"
                    elif 'rss' in msg.subject:
                        platform_name = "meteosat rss"
                    pass
                MESSAGE_START_TIME.labels(message_type=msg.type, topic=msg.subject, platform_name=platform_name).set(start_time)
                MESSAGE_END_TIME.labels(message_type=msg.type, topic=msg.subject, platform_name=platform_name).set(end_time)
                MESSAGE_REGISTER_TIME.labels(message_type=msg.type, topic=msg.subject, platform_name=platform_name).set_to_current_time()
                MESSAGE_NUMBER_OF_FILES.labels(message_type=msg.type, topic=msg.subject, platform_name=platform_name).set(number_of_files)
                MESSAGE_NUMBER_OF.labels(message_type=msg.type, topic=msg.subject, platform_name=platform_name).inc()

                _update_latest_status(msg, latest_status, platform_name, start_time, end_time, number_of_files)
                    
            elif msg.type == 'beat' or msg.type == 'info':
                try:
                    MESSAGE_REGISTER_TIME.labels(message_type=msg.type, topic=msg.subject, platform_name="NA").set_to_current_time()
                    MESSAGE_NUMBER_OF.labels(message_type=msg.type, topic=msg.subject, platform_name="NA").inc()
                except Exception as ex:
                    logger.warning("Got beat or info exception: %s", str(ex))
                    pass
            else:
                try:
                    logger.warning("Unknown message type:")
                    logger.warning("Data   : {}".format(msg.data))
                    logger.warning("Subject: {}".format(msg.subject))
                    logger.warning("Type   : {}".format(msg.type))
                    logger.warning("Sender : {}".format(msg.sender))
                    logger.warning("Time   : {}".format(msg.time))
                    logger.warning("Binary : {}".format(msg.binary))
                    logger.warning("Version: {}".format(msg.version))
                except Exception:
                    pass
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt. Shutting down.")
            break
        except SystemExit:
            logger.info("Received System Exit interrupt. Shutting down.")
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

def save_status_file(logger, status_file, latest_status):
    logger.info("Start writing status file.")
    with open(status_file, "wb") as ps:
        pickle.dump(latest_status, ps)
        logger.info(f"Wrote status file: {status_file}")
    
def signal_handler(signo, frame):
    print(f"signal_handler called with signal: {signo}")
    raise SystemExit

def main():
    '''Main. Parse cmdline, read config etc.'''

    args = arg_parse()

    config = None
    if os.path.exists(args.config_file):
        config = read_config(args.config_file, debug=False)

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

    # Create a metric from message key start_time
    start_http_server(config.get('prometheus_client_port', 8000))

    signal.signal(signal.SIGTERM, signal_handler)
    latest_status = {}
    status_file = "/tmp/latest-messages-prom-status"
    atexit.register(save_status_file, logger, status_file, latest_status)

    listener_queue = queue.Queue()
    startup_status = {}
    if os.path.exists(status_file):
         with open(status_file, "rb") as ps:
             startup_status = pickle.load(ps)
             logger.info("Read latest status file %s", status_file)
    else:
        logger.info("No latest status file found at startup: %s. Start with empty status.", status_file)
    listener = Listener(listener_queue, config, logger)
    listener.start()
    read_from_queue(listener_queue, logger, startup_status, latest_status)

    logger.info("Exit from read queue")
    
    logger.info("After message_handler.run()")
    listener.stop()
    logger.info("After queue_handler.terminate()")


if __name__ == "__main__":
    main()
