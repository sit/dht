#!/usr/bin/env python

import RPC
import socket
import sys
from traceback import print_exc
from bigint import bigint
import chord_types, dhash_types
import dhashgateway_prot
import time
import sha
import random
import asyncore
import getopt

# BY2BLK = the number of bytes in blocks at the root of the tree
#          DHash is optimized to store 8K blocks
BY2BLK = 8192

# convert a hexadecimal string into a bigint
def ascii2bigint(s):
    a = map(lambda x: int(x,16), s)
    v = 0L
    for d in a:
	v = v << 4
	v = v | d
    return v

class actor:
    def __init__ (self, client, k):
        self.client = client
	self.key = ascii2bigint(k)
        self.complete = 0

    def go (self):
	self.inject()
        # print "gone"
        if (self.complete == 1):
            self.client.close()

#fetches a block using its content hash and prints the block to standard output
#fetcher inherits from actor
class fetcher(actor):
    def inject (self):

        arg = dhashgateway_prot.dhash_retrieve_arg ()

	#specify the content hash key corresponding to the block to be retrieved
        arg.blockID = self.key 
        arg.ctype   = dhash_types.DHASH_CONTENTHASH

        arg.options = 0
        arg.guess   = bigint(0)
        
        start = time.time()
        try:
	    # invokes dhash gateway to fetch the block
	    # when the block is fetched, self.process() will be invoked
            res = self.client (dhashgateway_prot.DHASHPROC_RETRIEVE, arg,
                               lambda x: self.process(start, arg, x))
            if res is not None:
                self.process (start, arg, res)
        except RPC.UnpackException, e:
            print_exc ()

    def process (self, start, arg, res):
        end = time.time()
	self.complete    += 1
        if res.status != dhash_types.DHASH_OK:
            print arg.blockID, "retrieve failed!"
        else:
	    print res.resok.block
            print arg.blockID, '/', int((end - start) * 1000), '/', 
            for t in res.resok.times:
                print t,
            print '/', res.resok.errors, res.resok.retries, '/',
            print res.resok.hops,
            for id in res.resok.path:
                print id,
                
        self.go()

if __name__ == "__main__":
    try:
	opts, cmdv = getopt.getopt(sys.argv[1:], "h:n:f:s:o:")
    except getopt.GetoptError:
        # print help information and exit:
        print "fetch-block.py -h host:port -f key"
        sys.exit(2)

    for o, a in opts:
        if o == '-h':
            (host, sport) = a.split (":")
            port = int(sport)
            iofile = a
        if o == '-f':
            key = a

    try:
	#create a client dhashgateway to be connected to the chord ring at host:sport
        client = RPC.AClient(dhashgateway_prot,
                             dhashgateway_prot.DHASHGATEWAY_PROGRAM, 1,
                             host, port)
	a = fetcher (client, key)
    except (socket.error, EOFError, IOError), e:
        print_exc()
	sys.exit (1)

    def connectcb(s):
        if s is not None:
            print "Connected."
            a.go()
        else:
            print "Connect failed."

    #connect the client gateway to the chord ring
    client.start_connect(connectcb)

    #start fetching blocks
    asyncore.loop()
