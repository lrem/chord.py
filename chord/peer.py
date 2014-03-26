#!/usr/bin/env python3
"""
Chord peer
==========

This module provides peer of a Chord distributed hash table.
"""

import random
import time
import socket
import socketserver
import threading
import logging
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',
                    level=logging.DEBUG)

CHAIN = 3
CHORDS = 30
MAX_KEY = 2**CHORDS
CHORD_UPDATE_INTERVAL = 5


class Peer:

    def __init__(self, port=4321, key=None):
        if key is None:
            self.key = random.randint(0, MAX_KEY)
        else:
            self.key = key
        logging.info('Peer key: %x' % self.key)
        self.chords = [None] * CHORDS
        self.chain = [None]
        self.storage = {}
        self.port = port

    def connect(self, url):
        """
        Connects to the DHT using the given `url` (of any connected node).
        """
        logging.info('Connecting to: ' + url)
        old = self.find_re(self.key, connecting=url)
        logging.debug(old)
        self.chain = [old] + request(url, 'accept', self.key,
                                     bytes(str(self.port), 'ascii'))
        for i in range(CHORDS):
            key = (self.key + 2**i) % MAX_KEY
            if not inside(key, self.key, old[0]):
                self.chords[i] = self.find_re(key, connecting=url)

    def accept(self, key, url):
        """
        Accepts a peer to the DHT by:
        - putting him on the ring after itself
        - reassigning to him part of own key space
        """
        self.chain = [(key, url)] + self.chain
        # TODO: transfer him the stored keys
        for i in range(CHORDS):
            key = (self.key + 2**i) % MAX_KEY
            if self.chords[i] is None and\
                    not inside(key, self.key, self.chain[0][0]):
                self.chords[i] = self.chain[0]

    def start(self):
        """
        Starts Peer's operation.
        """
        Handler.peer = self
        logging.info('Listening on port %d' % self.port)
        server = Server(('0.0.0.0', self.port), Handler)
        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.daemon = True
        server_thread.start()
        logging.debug('Server thread started')
        while True:
            time.sleep(CHORD_UPDATE_INTERVAL)
            self._update_chords()

    def find(self, key):
        """
        Find a peer that is closer to the one responsible for the given `key`.
        Returns `None` if it's the responsible itself, or a tuple `(key, url)`.
        """
        if self.chain[0] is None or inside(key, self.key, self.chain[0][0]):
            return None
        for i in range(CHORDS - 1):
            if self.chords[i] is None:
                continue  # I'm still responsible for this part
            if inside(key, self.chords[i][0], self.chords[i+1][0]):
                return self.chords[i]
        if self.chords[-1] is None:
            return self.chain[0]  # Another funny corner case
        else:
            return self.chords[-1]

    def find_re(self, key, connecting=None):
        """
        Find the peer that is responsible for the given `key`.
        Returns `None` if it's the responsible itself, or a tuple `(key, url)`.
        """
        if connecting is not None:
            closer = (None, connecting)
        else:
            closer = self.find(key)
            if closer is None:
                return None
        while not isinstance(closer, Me):
            closer = request(closer[1], 'find', key)
        return closer

    def get(self, key):
        """
        Return the value for the `key`, wherever it is stored.
        """
        responsible = self.find_re(key)
        logging.debug('Peer %s responsible for key %x' %
                      (responsible, key))
        if responsible is None:
            return self.storage.get(key, None)
        else:
            return request(responsible[1], 'get', key)

    def put(self, key, value):
        """
        Store the `(key, value)` in the DHT.
        """
        responsible = self.find_re(key)
        logging.debug('Peer %s responsible for key %x' %
                      (responsible, key))
        if responsible is None:
            self.storage[key] = value
        else:
            request(responsible[1], 'put', key, value)

    def _update_chords(self):
        logging.info('Storing %d values' % len(self.storage))
        logging.debug(self.chain)
        if self.chain[0] is None:
            return
        logging.debug('Updating chords')
        for i in range(CHORDS):
            key = (self.key + 2**i) % MAX_KEY
            if not inside(key, self.key, self.chain[0][0]):
                self.chords[i] = self.find_re(key)
        logging.debug("%d chords established" %
                      sum([1 for x in self.chords if x is not None]))


def inside(key, left, right):
    """
    Find whether the key is in the interval `[left, right)`.
    Note the keys are arranged on a ring, so it is possible that left > right.
    """
    if left == right:
        return False
    if left < right:
        return left <= key < right
    else:
        return left <= key or key < right


def request(url, operation, key, value=None):
    logging.debug('Requesting from %s operation %s key %x value %s' %
                  (url, operation, key, value))
    sock = _connect(url)
    body = bytes("%s %x\n" % (operation, key), 'ascii')
    if value:
        body += bytes("%d\n" % len(value), 'ascii')
        body += value
    try:
        sock.sendall(body)
        inh = sock.makefile('rb')
        response = inh.readline()
        if response.startswith(b'value'):
            logging.debug(response)
            length = int(response.split()[1])
            return inh.read(length)
        elif response.startswith(b'none'):
            raise KeyError("Key %x not in DHT" % key)
        elif response.startswith(b'peer'):
            logging.debug('Raw response %s' % response)
            return _parse_peer(response)
        elif response.startswith(b'me'):
            key = int(response.split()[1], base=16)
            return Me([key, url])
        elif response.startswith(b'chain'):
            chain = []
            for line in inh:
                chain.append(_parse_peer(line))
            return chain
    finally:
        sock.close()
    return response


class Handler(socketserver.StreamRequestHandler):

    peer = None

    def handle(self):
        inh = self.rfile
        operation, key = inh.readline().split()
        key = int(key, base=16)
        logging.info("Request: %s %x" % (operation, key))
        response = b'unknown operation'
        if operation == b'find':
            peer = self.peer.find(key)
            if peer is None:
                response = bytes("me %x\n" % self.peer.key, 'ascii')
            else:
                response = _serialize_peer(peer)
        elif operation == b'accept':
            response = b"chain\n"
            for peer in self.peer.chain:
                response += _serialize_peer(peer)
            port = int(_read_value(inh))
            self.peer.accept(key, _make_url(self.request, port))
        elif operation == b'get':
            value = self.peer.get(key)
            if value is None:
                response = b'none'
            else:
                response = bytes("value %d\n" % len(value), 'ascii')
                response += value
        elif operation == b'put':
            value = _read_value(inh)
            logging.debug("Value: %s" % value)
            self.peer.put(key, value)
            response = b'ok'
        elif operation == b'ping':
            response = b'pong'
        logging.debug("Response: %s\n" % response)
        self.request.sendall(response)


def _read_value(inh):
    length = int(inh.readline())
    return inh.read(length)


class Server(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass


class Address(tuple):  # Hate I can't define my own __init__
    pass


class Me(Address):
    pass


def _parse_peer(line):
    if line.startswith(b'peer'):
        key, url = line.split()[1:]
        return Address([int(key, base=16), url])
    elif line.startswith(b'none'):
        return None
    else:
        raise ValueError('Wrong response for peer %s' % line)


def _serialize_peer(peer):
    if peer is None:
        return b'none'
    else:
        return bytes("peer %x %s\n" % (peer[0], str(peer[1], 'ascii')),
                     'ascii')


def _make_url(socket, port=None):
    #FIXME: this gives us the request socket, not the listening one
    if port is None:
        return bytes("%s:%d" % socket.getpeername(), 'ascii')
    else:
        return bytes("%s:%d" % (socket.getpeername()[0], port), 'ascii')


def _connect(url):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if isinstance(url, bytes):
        url = str(url, 'ascii')
    if ':' in str(url):
        host, port = url.split(':')
        port = int(port)
    else:
        host, port = url, 4321
    sock.connect((host, port))
    return sock


def main():
    import argparse
    argp = argparse.ArgumentParser(description=__doc__)
    argp.add_argument('-key', help='hexadecimal key for this node')
    argp.add_argument('-url', help='url of an existing DHT peer')
    argp.add_argument('-port', help='listening TCP port',
                      type=int, default=4321)
    args = argp.parse_args()
    if args.key is not None:
        args.key = int(args.key, 16)
    peer = Peer(port=args.port, key=args.key)
    if args.url:
        peer.connect(args.url)
    peer.start()

if __name__ == '__main__':
    main()
