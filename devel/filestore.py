#!/usr/bin/env python

import RPC
import socket
import sys
from traceback import print_exc
import chord_types
import dhash_types
import dhashgateway_prot
import time
import sha
import random
import asyncore

H2BLK = 4
BY2BLK = 8192

def ascii2bigint(s):
    a = map(lambda x: int(x,16), s)
    v = 0L
    for d in a:
	v = v << 4
	v = v | d
    return v

class fetcher:
    def __init__ (self, client, rootkey, levels, nops):
        self.client = client
	self.nops = nops
	self.keys = [(ascii2bigint(rootkey), int(levels), 0)]
	self.f = open("out", "w")
	self.outstanding = 0
        self.complete = 0

    def go(self):
	self.getnextblock()

    def getnextblock (self):
	if (len(self.keys) == 0):
	    print "everything is done! happy kitty:)"
	    self.client.close()
	    return

        arg = dhashgateway_prot.dhash_retrieve_arg ()
	(id, level, off) = self.keys.pop()
        arg.blockID = id
        arg.ctype   = dhash_types.DHASH_CONTENTHASH
        arg.options = 0
        arg.guess   = chord_types.bigint(0)

        start = time.time()
        try:
            self.outstanding += 1
            res = self.client (dhashgateway_prot.DHASHPROC_RETRIEVE, arg,
                               lambda x: self.process(start, arg, level, off, x))
            if res is not None:
#                self.process (start, arg, res)
#                above seems like a copy paste error
                 sys.exit (-1)                
        except RPC.UnpackException, e:
            print_exc ()

	print "%x start retrieving" % id

    def process (self, start, arg, level, off, res):
        print " in process status %d" % res.status
        self.outstanding -= 1
        self.complete    += 1
        if res.status != dhash_types.DHASH_OK:
            print "%x retrieve failed!" % arg.blockID
        else:
            blk = res.resok.block
            for t in res.resok.times:
                print t,
            print '/', res.resok.errors, res.resok.retries, '/',
            print res.resok.hops,
            for id in res.resok.path:
                print id,

	    if level == 0: #data block
	        self.f.seek(off)
		self.f.write(blk)
	    else:   
	        s = 0
	        offset = off
		assert(res.resok.len >= 20)
	        while s < res.resok.len:
		    hash = blk[s:s+20]
		    print chord_types.str2bigint(hash), " offset ", offset, " level ", level-1
		    self.keys.append((chord_types.str2bigint(hash), level-1, offset))
		    s += 20
		    offset += (H2BLK**(level - 1))*BY2BLK
        self.getnextblock()

class fileobj:
    def __init__(self, filename):
	self.f = open(filename, "rb")
	self.level = 0
	self.hashes = []
	self.nexthash = []
        self.eoflag = False
        self.rootkey = ""

    def next_block(self):
	if self.level == 0:
           data = self.f.read(BY2BLK)
           if len(data) == 0:
               self.level += 1
           else:
               self.hashes.append(sha.sha(data))
	       sd = sha.sha(data).digest()
	       print "hash is ", chord_types.str2bigint(sd), " offset ", self.f.tell()-len(data), " sz", len(data)
	       return data
        if self.level > 0: 
	   hashdata = ""
	   hnum = 0
	   while len(self.hashes) > 0 and hnum < H2BLK:
	       hashdata += self.hashes[0].digest()
	       self.hashes.remove(self.hashes[0])
               hnum += 1
	   self.nexthash.append(sha.sha(hashdata))        
	   if len(self.hashes) == 0 and len(self.nexthash) == 1:
	       self.rootkey = self.nexthash[0].digest()
               self.eoflag = True
           elif len(self.hashes) == 0:
               self.hashes = self.nexthash
	       self.nexthash = []
	       self.level += 1
	   return hashdata
        
    def eof(self):
        return self.eoflag       

    def root(self):
        return self.rootkey

    def rootlevel(self):
        return self.level    


class actor:
    def __init__ (self, client, f, nops):
        self.client = client
        self.nops = nops
        self.complete = 0
        self.outstanding = 0
        self.going = 0
	self.f = f

    def go (self):
        if self.going: return
        # print "going out: %d comp: %d" % (self.outstanding, self.complete)
        self.going = 1
        while (self.outstanding < self.nops and self.f.eof() == 0):
            self.inject()
            # print "injected out: %d comp: %d" % (self.outstanding, self.complete)
        self.going = 0
        # print "gone"
        if self.outstanding == 0 and self.f.eof() == True:
            #sys.exit(0)
	    print str(chord_types.bigint(self.f.root())) + ":" + str(self.f.rootlevel())
            self.client.close()


class storer (actor):
    def inject (self):
        arg = dhashgateway_prot.dhash_insert_arg ()
        
        arg.block   = self.f.next_block()
        sobj        = sha.sha(arg.block)
        arg.blockID = chord_types.bigint(sobj.digest())
        print "Inserting", arg.blockID
        arg.ctype   = dhash_types.DHASH_CONTENTHASH
        arg.len     = len(arg.block)
        arg.options = 0
        arg.guess   = chord_types.bigint(0)
        
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
    if len(sys.argv) < 6:
        print """usage: dbm host port file <f or s> nops
        host and port indicate where lsd is listening for RPCs
        file is the file to insert
        <f or s> selects fetch or store
        nops indicates number of parallel operations to use
        seed is the seed for the PRNG"""
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    mode = sys.argv[4]
    nops = int(sys.argv[5])
    
    if mode not in ['f', 's']:
        sys.stderr.write ("Unknown mode '%s'; bailing.\n" % (mode))
        sys.exit (1)
    
    # XXX redirect stdout to point to whereever file wants you to go.
    try:
        client = RPC.AClient(dhashgateway_prot,
                             dhashgateway_prot.DHASHGATEWAY_PROGRAM, 1,
                             host, port)
        if mode == 'f':
	    (root, level) = sys.argv[3].split(":")
            print root
	    print "level %d " % int(level)
	    a = fetcher (client, root, level, nops)
        elif mode == 's':
            file = fileobj(sys.argv[3])
            a = storer (client, file, nops)
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
