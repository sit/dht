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
import getopt

# H2BLK defines the number of hashes per interior block
#  this is the branching factor of the tree
H2BLK = 4

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

# fetcher: download the file named by rootkey
#          file is stored as a venti-style tree
#  args:
#     levels - height of Venti tree
#     nops - number of operations to do in parallel (ignored!)
#     rootkey - ID of the block at the top of the tree
#     client - async RPC object
class fetcher:
    def __init__ (self, client, rootkey, levels, iofile, nops):
        self.client = client
	self.nops = nops
        # keys holds the IDs of blocks we need to fetch
        # could be: rootblock, internal block IDs, data block IDs...
	self.keys = [(ascii2bigint(rootkey), int(levels), 0)]
	self.f = open(iofile, "w")
        self.outstanding = 0
        self.complete = 0
        
    def go(self):
        self.getnextblock()

    # get the next block the reschedule myself to get another
    def getnextblock (self):
        #if no keys left to fetch, exit
	if (len(self.keys) == 0):
	    print "done."
	    self.client.close()
	    return

        # setup the argument for the call to DHash
        arg = dhashgateway_prot.dhash_retrieve_arg ()
        # get the next key from the array
	(id, level, off) = self.keys.pop()
        arg.blockID = id
        arg.ctype   = dhash_types.DHASH_CONTENTHASH
        arg.options = 0
        arg.guess   = chord_types.bigint(0)

        # we time each fetch for diagnostics: record start time
        start = time.time()
        try:
            self.outstanding += 1
            # the RPC client will call self.process when the block comes back
            res = self.client (dhashgateway_prot.DHASHPROC_RETRIEVE, arg,
                               lambda x: self.process(start, arg, level, off, x))
            if res is not None:
                 # something went badly wrong
                 sys.exit (-1)
                 
        except RPC.UnpackException, e:
            print_exc ()

	print "start retrieving %x" % id


    # process a returned block
    # args:
    #   start: start time
    #   arg: argument used to find this block
    #   level: level block was retrieved from
    #   off: offset of this block in the file
    #   res: result of RPC
    def process (self, start, arg, level, off, res):
        self.outstanding -= 1
        self.complete    += 1
        if res.status != dhash_types.DHASH_OK:
            print "%x retrieve failed!" % arg.blockID
        else:
            blk = res.resok.block
            # print some diagnostics
            for t in res.resok.times:
                print t,
            print '/', res.resok.errors, res.resok.retries, '/',
            print res.resok.hops,
            for id in res.resok.path:
                print id,

            # this was a data block, write the info to the output file
	    if level == 0:
	        self.f.seek(off)
		self.f.write(blk)
	    else:
                # internal node
	        s = 0
	        offset = off
		assert(res.resok.len >= 20)
                # add all of the keys in this node to the
                # list of keys to be fetched
	        while s < res.resok.len:
		    hash = blk[s:s+20]
		    self.keys.append((chord_types.str2bigint(hash),
                                      level-1, offset))
		    s += 20
                    # update the offset for each key
		    offset += (H2BLK**(level - 1))*BY2BLK

        # get another block
        self.getnextblock()


# fileobj: convert a file into a tree
class fileobj:
    def __init__(self, filename):
	self.f = open(filename, "rb")
	self.level = 0
	self.hashes = []
	self.nexthash = []
        self.eoflag = False
        self.rootkey = ""

    # return a block from the tree
    def next_block(self):
	if self.level == 0:
           data = self.f.read(BY2BLK)
           if len(data) == 0:
               self.level += 1
           else:
               self.hashes.append(sha.sha(data))
	       sd = sha.sha(data).digest()
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

    # returns true if the entire tree has been output
    def eof(self):
        return self.eoflag       

    # below functions only valid if eoflag == true
    # returns the key of the root of the tree
    def root(self):
        return self.rootkey

    # returns the height of the tree
    def rootlevel(self):
        return self.level    


#helper class for storer (and perhaps a high-performance fetcher?)
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
        self.going = 1
        while (self.outstanding < self.nops and self.f.eof() == 0):
            self.inject()

        self.going = 0
        if self.outstanding == 0 and self.f.eof() == True:
            #output root key and level
	    print "file root key ", str(chord_types.bigint(self.f.root())) + ":" + str(self.f.rootlevel())
            self.client.close()

# store a file (inherits from actor)
class storer (actor):
    def inject (self):
        arg = dhashgateway_prot.dhash_insert_arg ()

        # get the next block and insert it
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

    # process the result of the RPC
    # (mainly only error codes)
    def process (self, start, arg, res):
        end = time.time()
        self.outstanding -= 1
        self.complete += 1
        if res.status != dhash_types.DHASH_OK:
            print "Insert failed, key:", arg.blockID
        else:
	    print "Inserted key:", arg.blockID, "in", int((end - start) * 1000), "msec path:",
            for id in res.resok.path:
                print id,
            print
        self.go()

# main
def usage ():
    # print help information and exit:
    print "Usage: filestore [-n nops] -h host:port [-f key:level -o output| -s file]"
    sys.exit(2)

if __name__ == "__main__":
    try:
        opts, cmdv = getopt.getopt(sys.argv[1:], "h:n:f:s:o:")
    except getopt.GetoptError:
	usage ()

    nops = 1
    for o, a in opts:
        if o == '-h':
            (host, sport) = a.split (":")
            port = int(sport)
        if o == '-s':
            mode = 's'
            iofile = a
        if o == '-o':
            iofile = a
            print "will write output to %s" % iofile
        if o == '-f':
            mode = 'f'
            (root, level) = a.split(":")
        if o == '-n':
            nops = int(a)
    
    try:
	if mode not in ['f', 's']:
	    sys.stderr.write ("Unknown mode '%s'; bailing.\n" % (mode))
	    sys.exit (1)
    except:
	usage ()
    
    #create the client
    try:
        client = RPC.AClient(dhashgateway_prot,
                             dhashgateway_prot.DHASHGATEWAY_PROGRAM, 1,
                             host, port)
        if mode == 'f':
	    a = fetcher (client, root, level, iofile, nops)
        elif mode == 's':
            file = fileobj(iofile)
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
