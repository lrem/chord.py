"""
This test needs an overlay with peers in 'localhost:432[12]' running.
"""

import chord.peer
URL = 'localhost:4321'
URL2 = 'localhost:4322'
KEYS = 16


def test_ping():
    """
    See whether the overlay responds to pings.
    """
    response = chord.peer.request(URL, 'ping', 0)
    assert response == b'pong', response


def test_put_get():
    """
    Put uniformly distributed keys, see if can get them back.
    """
    increment = chord.peer.MAX_KEY // KEYS
    key = lambda i: i * increment
    for i in range(KEYS):
        _put(key(i), bytes("%x" % key(i), 'ascii'))
    for i in range(KEYS):
        _get(key(i), bytes("%x" % key(i), 'ascii'))


def test_put_get_elsewhere():
    """
    Put uniformly distributed keys, see if can get them back
    using other entry point.
    """
    increment = chord.peer.MAX_KEY // KEYS
    key = lambda i: i * increment + 1
    for i in range(KEYS):
        _put(key(i), bytes("%x" % key(i), 'ascii'))
    for i in range(KEYS):
        _get(key(i), bytes("%x" % key(i), 'ascii'), url=URL2)


def _put(key, value, url=URL):
    """
    A single `put` request, should always return `ok`.
    """
    response = chord.peer.request(URL, 'put', key, value)
    assert response == b'ok', response


def _get(key, expected, url=URL):
    """
    A single `get` request, checking if obtained value equals `expected`.
    """
    response = chord.peer.request(url, 'get', key)
    assert response == expected, (response, expected)
