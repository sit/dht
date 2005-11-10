#!/usr/bin/env python
import bisect
import random
import string
import sys
from utils import random_id, random_interval

initial_nodes = 200

mttr   = 172218  # time steps to repair
mttf   = 891887  # time steps to failure
sd     = mttf/2

pc     = 0.2

stop_time = 86400 * 7 * 26
if len (sys.argv) > 1:
    stop_time = int (sys.argv[1])

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
    add_event (random_interval (86400, sd/2), "join", nnode)
nnode = long(events[0].args)

total_insert = 4000
for t in xrange(86400,21*86400,300):
    ni = random_interval (50, 5)
    while total_insert > 0 and ni > 0:
	add_event (t, "insert", nnode, random_id (), 20*1024*1024)
	ni -= 1
	total_insert -= 1
            
ev = events.pop (0)
for t in xrange(0,stop_time):
    while ev.time == t:
	if ev.type == "fail" or ev.type == "crash":
	    # Initiate repair!
	    nt = random_interval (mttr, sd)
	    add_event (t + nt, "join", ev.args)
        elif ev.type == "join":  # give some startup time.
            # Schedule failure.
	    nt = random_interval (mttf, sd)
            type = "fail"
            flip = random.random ()
            if flip < pc: type = "crash"
            add_event (t + nt, type, ev.args)
        print ev
        try: ev = events.pop (0)
        except: break

