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

def make_block (sz):
    rd = ""
    csz = sz;
    while csz > 0:
        rd += chr(random.randint(1,255)) # randint is inclusive
        csz -= 1
    return rd

class actor:
    def __init__ (self, client, num_trials, data_size, nops):
        self.client = client
        self.num_trials = num_trials
        self.data_size = data_size
        self.nops = nops
        self.complete = 0
        self.outstanding = 0
        self.going = 0

    def go (self):
        if self.going: return
        # print "going out: %d comp: %d" % (self.outstanding, self.complete)
        self.going = 1
        while (self.outstanding < self.nops and
               self.complete + self.outstanding < self.num_trials):
            self.inject()
            # print "injected out: %d comp: %d" % (self.outstanding, self.complete)
        self.going = 0
        # print "gone"
        if self.outstanding == 0 and self.complete == self.num_trials:
            #sys.exit(0)
            self.client.close()

class fetcher(actor):
    def inject (self):
        arg = dhashgateway_prot.dhash_retrieve_arg ()
        blk = make_block (data_size)
        sobj = sha.sha(blk)
        arg.blockID = bigint(sobj.digest())
        arg.ctype   = dhash_types.DHASH_CONTENTHASH
        arg.options = 0
        arg.guess   = bigint(0)
        
        start = time.time()
        try:
            self.outstanding += 1
            res = self.client (dhashgateway_prot.DHASHPROC_RETRIEVE, arg,
                               lambda x: self.process(start, arg, blk, x))
            if res is not None:
                self.process (start, arg, res)
        except RPC.UnpackException, e:
            print_exc ()

    def process (self, start, arg, blk, res):
        end = time.time()
        self.outstanding -= 1
        self.complete    += 1
        if res.status != dhash_types.DHASH_OK:
            print arg.blockID, "retrieve failed!"
        else:
            err = ''
            if blk != res.resok.block:
                err += "mismatched block, "
		if arg.ctype != res.resok.ctype:
		    err += "mismatched ctype, "
                if data_size != res.resok.len:
                    err += "incorrect length (%d not %d), " % (res.resok.len, data_size)
                if len(err):
                    err = err[:-2]
            print arg.blockID, '/', int((end - start) * 1000), '/', 
            for t in res.resok.times:
                print t,
            print '/', res.resok.errors, res.resok.retries, '/',
            print res.resok.hops,
            for id in res.resok.path:
                print id,
            print err
                
        self.go()

class storer (actor):
    def inject (self):
        arg = dhashgateway_prot.dhash_insert_arg ()
        
        arg.block   = make_block (data_size)
        sobj = sha.sha(arg.block)
        arg.blockID = bigint(sobj.digest())
        print "Inserting", arg.blockID
        arg.ctype   = dhash_types.DHASH_CONTENTHASH
        arg.len     = data_size
        arg.options = 0
        arg.guess   = bigint(0)
        
        start = time.time()
        try:
            self.outstanding += 1
            res = self.client (dhashgateway_prot.DHASHPROC_INSERT, arg,
                               lambda x: self.process(start, arg, x))
            if res is not None:
                self.process (start, arg, res)
        except RPC.UnpackException, e:
            print_exc ()

    def process (self, start, arg, res):
        end = time.time()
        self.outstanding -= 1
        self.complete += 1
        print arg.blockID, int((end - start) * 1000),
        if res.status != dhash_types.DHASH_OK:
            print "insert failed!"
        else:
            for id in res.resok.path:
                print id,
            print
        self.go()

if __name__ == "__main__":
    if len(sys.argv) < 9:
        print """usage: dbm host port num_trials data_size file <f or s> nops seed
        host and port indicate where lsd is listening for RPCs
        num_trials is the number of inserts or fetches to perform
        data_size is the size of each block
        file indicates where output should go
        <f or s> selects fetch or store
        nops indicates number of parallel operations to use
        seed is the seed for the PRNG"""
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    num_trials = int(sys.argv[3])
    data_size = int(sys.argv[4])
    file = sys.argv[5]
    mode = sys.argv[6]
    nops = int(sys.argv[7])
    seed = int(sys.argv[8])
    
    random.seed (seed)

    if mode not in ['f', 's']:
        sys.stderr.write ("Unknown mode '%s'; bailing.\n" % (mode))
        sys.exit (1)
    
    # XXX redirect stdout to point to whereever file wants you to go.
    try:
        client = RPC.AClient(dhashgateway_prot,
                             dhashgateway_prot.DHASHGATEWAY_PROGRAM, 1,
                             host, port)
        if mode == 'f':
            a = fetcher (client, num_trials, data_size, nops)
        elif mode == 's':
            a = storer (client, num_trials, data_size, nops)
    except (socket.error, EOFError, IOError), e:
        print_exc()
	sys.exit (1)

    def connectcb(s):
        if s is not None:
            print "Connected."
            a.go()
        else:
            print "Connect failed."
    client.start_connect(connectcb)
    asyncore.loop()
