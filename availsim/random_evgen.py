#!/usr/bin/env python
import bisect
import random
import sys

initial_nodes = 200
nbits = 20

mttr   = 1000.0 # time steps to repair
mttf   = 1000.0 # time steps to failure
sd     = 700.0  # stddev of above times

pc     = 0.10  # chance of a failure being a complete crash

mtti   = 10    # average time steps between insert events
sdi    = 5

stop_time = 1000
if len (sys.argv) > 1:
    stop_time = int (sys.argv[1])

def random_id ():
    return random.randrange (0, 2**nbits)

def random_interval (mean, sd):
    return max (0, int (random.gauss (mean, sd)))

class event:
    def __init__ (my, t, type, args):
        my.time = t
        my.type = type
        my.args = args
    def __repr__(my):
	return "%d\t%s\t%s" % (my.time, my.type, my.args)
    def __cmp__(my, other):
	return cmp(my.time, other.time)
    
events = []
def insert_event (t, type, args):
    bisect.insort (events, event (t, type, args))

maxid = 0
nodes = {}
for i in xrange(0,initial_nodes):
    nnode = random_id ()
    while nnode in nodes:
        nnode = random_id ()
    nodes[nnode] = 1
    insert_event (random_interval (mttr, sd), "join", nnode)

insert_event (mttr + random_interval (mtti, sdi), "insert", random_id ())

ev = events.pop (0)
for t in xrange(0,stop_time):
    while ev.time == t:
	if ev.type == "fail" or ev.type == "crash":
	    # Initiate repair!
	    nt = random_interval (mttr, sd)
	    insert_event (t + nt, "join", ev.args)
        elif ev.type == "join":
            # Schedule failure.
	    nt = random_interval (mttf, sd)
            type = "fail"
            flip = random.random ()
            if flip < pc: type = "crash"
            insert_event (t + nt, type, ev.args)
        elif ev.type == "insert":
            nt = random_interval (mtti, sdi)
            insert_event (t + nt, "insert", random_id ())
        print ev
        try: ev = events.pop (0)
        except: break
        

