#!/usr/bin/env python
import bisect
import random
import string
import sys

initial_nodes = 200
nbits = 20

mttr   = 60.0 # time steps to repair
mttf   = 60.0 # time steps to failure
sd     = mttr/2  # stddev of above times

pc     = 0.10  # chance of a failure being a complete crash

mu_i   = 10    # number of blocks inserted per timestep
sd_i   = 10


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
        my.args = string.join(map(lambda x: "%s" % x, args), "\t")
    def __repr__(my):
	return "%d\t%s\t%s" % (my.time, my.type, my.args)
    def __cmp__(my, other):
	return cmp(my.time, other.time)
    
events = []
def add_event (t, type, *args):
    bisect.insort (events, event (t, type, args))

maxid = 0
nodes = {}
for i in xrange(0,initial_nodes):
    nnode = random_id ()
    while nnode in nodes:
        nnode = random_id ()
    nodes[nnode] = 1
    add_event (random_interval (mttr/2, sd/2), "join", nnode)
nnode = long(events[0].args)

for t in xrange(0,stop_time):
    # After joins happen, insert a bunch of blocks.
    # Repair happens at the end of each tick anyway.
    if t > mttr:
        ni = random_interval (mu_i, sd_i)
        while ni > 0:
            add_event (t, "insert", nnode, random_id ())
            ni -= 1
            
ev = events.pop (0)
for t in xrange(0,stop_time):
    while ev.time == t:
	if ev.type == "fail" or ev.type == "crash":
	    # Initiate repair!
	    nt = random_interval (mttr, sd)
	    add_event (t + nt, "join", ev.args)
        elif ev.type == "join" and t > mttf:  # give some startup time.
            # Schedule failure.
	    nt = random_interval (mttf, sd)
            type = "fail"
            flip = random.random ()
            if flip < pc: type = "crash"
            add_event (t + nt, type, ev.args)
        print ev
        try: ev = events.pop (0)
        except: break

