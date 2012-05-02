#! /usr/bin/env python

import base64
import json
import logging
import socket
import time
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream, SSLIOStream

try:
    from logging.handlers import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass

HOST = "stream.twitter.com"
PORT = 443

class Config(object):
    def __init__(self, username=None, password=None, consumer_token=None,
            consumer_secret=None, logging_enabled=True, log_level='INFO',
            log_file='twitter_stream.log', logger_name=None, **kwargs):
        self.username = username
        self.password = password
        self.consumber_token = consumer_token
        self.consumer_secret = consumer_secret
        self.logging_enabled = logging_enabled
        self.log_level = log_level
        self.log_file = log_file or 'twitter_stream.log'
        self.logger_name = logger_name or __name__
        self.__dict__.update(kwargs)

class TwitterStream(object):
    """
    A class for consuming data from Twitter's realtime streaming api.
    """

    def __init__(self, config, ioloop=None, path=None, callback=None,
            reconnect_on_close=True):
        self._ioloop = ioloop or IOLoop.instance() 
        self._sock = None
        self._sock_addr = None
        self._init_socket() #initalizes above variables
        self._stream = None
        self._reconnect_on_close = reconnect_on_close
        self._path = path or '/1/statuses/sample.json'
        self._username = config.username
        self._password = config.password
        self.callback = callback

        logging.basicConfig(filename=config.log_file,
                level=config.log_level)
        self.logger = logging.getLogger(config.logger_name)
        self.logger.setLevel(config.log_level)
        if self.logging_enabled:
            # TODO: Allow for more control over configuration
            handler = logging.handlers.TimedRotatingFileHandler(
                    filename=self.log_file,
                    when='midnight',
                    backupCount=7)
            self.logger.addHandler(handler)
        else:
            self.logger.addHandler(NullHandler())


    def _init_socket(self):
        addr_info = socket.getaddrinfo(HOST, PORT, socket.AF_INET, 
                socket.SOCK_STREAM)[0] #returns a list
        sock_fam, sock_type, proto = addr_info [:3]
        self._sock_addr = addr_info[-1]
        self._sock = socket.socket(sock_fam, sock_type, proto)

    def _get_default_headers(self):
        return {
            "Host": "stream.twitter.com",
            #"Accept-Encoding": "gzip, deflate", #TODO: Check tornado docs 
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "python-tornado"
        }

    def _build_basic_auth_header(self):
        assert self._username and self._password, \
                "Username and password required"
        auth_str = base64.encodestring(
                '%s:%s' % (self._username,self._password)).strip()
        return {
                'Authorization': 'Basic %s' % auth_str
        }

    def _build_oauth_header(self):
        assert self._consumer_token and self._consumer_secret, \
                "Consumer token and secret required"
        return {}

    def get_request_header(self):
        # Sticking to basic auth for testing. Will implement OAuth
        # later.
        auth_header = self._build_basic_auth_header() #TODO: OAuth
        headers = self._get_default_headers()
        headers.update(auth_header)
        base_request = "GET %s HTTP/1.1" % self._path
        request_parts = [base_request]
        for k, v in headers.iteritems():
            request_parts.append('%s: %s' % (k, v))
        request_parts.append('\r\n') #end of the request
        request = '\r\n'.join(request_parts)
        return request

    def on_close(self):
        # TODO: some sort of back-off or throttling in case twitter is down
        if self._reconnect_on_close:
            self.logger.info('Connection lost. Reconnecting...')
            # tidy up, just in case...
            self._stream.close()
            self._sock.close()
            self._stream = None
            self._sock = None
            self._counter = 0

            # re-init everything
            self._init_socket()
            self.connect()

    def on_error(self, error):
        self.logger.error('Error occurred: %s', error, exc_info=True)
        raise error

    def load_message(self, message):
        if not message.strip(): return
        try:
            msg = json.loads(message)
            return msg
        except Exception, e:
            self.logger.error('Malformed message: %s' % message)

    def on_message(self, message):
        msg = self.load_message(message)
        if self.callback and msg: 
            self.callback(msg)
        self._stream.read_until('\r\n', callback=self.on_message_len)

    def on_message_len(self, msg_len):
        msg_len = msg_len.strip()
        if msg_len == "": #might be blank (keepalive, per the docs)
            self._stream.read_until('\r\n', callback=self.on_message_len)
            return
        length = int(msg_len, base=16) 
        self._stream.read_bytes(length, callback=self.on_message)

    def on_resp(self, response):
        resp_status = response.splitlines()[0]
        status_code = int(resp_status.split()[1])
        if status_code != 200:
            e = Exception("Error connecting to stream: \n%s" % response)
            self.on_error(e)
        self._stream.read_until('\r\n', callback=self.on_message_len)

    def on_connect(self):
        self._stream.set_close_callback(self.on_close)
        request = self.get_request_header()
        self._stream.write(request)
        self._stream.read_until('\r\n\r\n', callback=self.on_resp)

    def connect(self):
        self._stream = SSLIOStream(self._sock, io_loop=self._ioloop)
        self._stream.connect(self._sock_addr, callback=self.on_connect)

    def start(self):
        self.connect()
        IOLoop.instance().start()


if __name__ == "__main__":
    stream = TwitterStream()
    stream.start()
