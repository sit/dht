#!/usr/bin/env python

import RPC
import socket
import sys
from traceback import print_exc
import chord_types, dhash_types
import dhashgateway_prot
import time
import sha
import random

def make_block (sz):
    rd = ""
    csz = sz;
    while csz > 0:
        rd += chr(random.randint(1,255)) # randint is inclusive
        csz -= 1
    return rd

def dofetch (client, num_trials, data_size, nops, seed):
    complete = 0
    while complete < num_trials:
	try:
	    arg = dhashgateway_prot.dhash_retrieve_arg ()
	    blk = make_block (data_size)
	    sobj = sha.sha(blk)
	    arg.blockID = chord_types.bigint(sobj.digest())
	    arg.ctype   = dhash_types.DHASH_CONTENTHASH
	    arg.options = 0
	    arg.guess   = chord_types.bigint(0)

	    start = time.time()
	    res   = client (dhashgateway_prot.DHASHPROC_RETRIEVE, arg)
	    end   = time.time()
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
	    pass
	except RPC.UnpackException, e:
	    print_exc()
	complete += 1

def dostore (client, num_trials, data_size, nops, seed):
    complete = 0
    while complete < num_trials:
	try:
	    arg = dhashgateway_prot.dhash_insert_arg ()

	    arg.block   = make_block (data_size)
	    sobj = sha.sha(arg.block)
	    arg.blockID = chord_types.bigint(sobj.digest())
	    print "Inserting", arg.blockID
	    arg.ctype   = dhash_types.DHASH_CONTENTHASH
	    arg.len     = data_size
	    arg.options = 0
	    arg.guess   = chord_types.bigint(0)

            start = time.time()
	    res = client (dhashgateway_prot.DHASHPROC_INSERT, arg)
	    end   = time.time()
	    if res.status != dhash_types.DHASH_OK:
		print arg.blockID, "insert failed!"
	    else:
		print arg.blockID, int((end - start) * 1000),
		for id in res.resok.path:
		    print id,
		print
	except RPC.UnpackException, e:
	    print_exc()
	complete += 1


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
    
    if nops != 0:
        sys.stderr.write ("nops is currently ignored.\n")

    random.seed (seed)
    
    # XXX redirect stdout to point to whereever file wants you to go.
    try:
        client = RPC.Client(dhashgateway_prot,
                            dhashgateway_prot.DHASHGATEWAY_PROGRAM, 1,
                            host, port)
        print "Connected!"
    except (socket.error, EOFError, IOError), e:
        print_exc()
	sys.exit (1)
        
    try:
        if mode == 'f':
            dofetch (client, num_trials, data_size, nops, seed)
        elif mode == 's':
            dostore (client, num_trials, data_size, nops, seed)
        else:
            sys.stderr.write ("Unknown mode '%s'; bailing.\n" % (mode))
            sys.exit (1)
    except (socket.error, EOFError, IOError), e:
        print_exc()
