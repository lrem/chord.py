chord.py
========

A pure Python key-value store implementing the Chord algorithm.
Chord is a classic Distributed Hash Table (DHT).
This is a *prototype*, 
made as an exercise in lower level networking using sockets and threads.
You can start an arbitrary number of storage nodes
and `put`/`get` values.
They should get nicely distributed among the nodes.

However, there are crucial features missing:
 - support for nodes leaving the DHT
 - data redundancy
 - migrating keys to new nodes

I might implement those, if I ever find the motivation.
I can also accept contributions.
The code is covered by the MIT license.
