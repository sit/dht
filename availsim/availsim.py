#!/usr/bin/env python

from __future__ import generators
import sys

from utils import str2chordID, size_rounder
import dhash

def get_node_id (args):
    try:
	return long(args)
    except:
	try:
	    return str2chordID (args)
	except:
	    raise SyntaxError, "Bad ID specification"
        
class event:
    def ev_getnode (my, args):
        id = get_node_id (args[0])
        my.id = id

    def ev_getblock (my, args):
        my.id = get_node_id (args[0])
        try:
            id = long (args[1])
        except:
            id = str2chordID (args[1])
        my.block = id
        # size is optional
        try:
            my.size = int(args[2])
        except:
            my.size = 8192
    
    def __init__ (my, time, type, *args):
        my.time = time
        my.type = type
                
        etypes = {
            'join'  : my.ev_getnode,
            'fail'  : my.ev_getnode,
            'crash' : my.ev_getnode,
            'insert': my.ev_getblock
        }
        if type not in etypes:
            raise TypeError, "Unknown event type"
	apply (etypes[type], args)
        
    def __str__ (my):
        return "%ld %s" % (time, type)

class simulator:
    def __init__ (my, dht):
        my.dh = dht
        
    def run (my, evgen, monitor):
        last_time = 0
        for ev in evgen:
            assert last_time <= ev.time, "Woah! Time can't go backwards. Bye."
            # Call the monitor before the time changes.
            if last_time != ev.time:
                my.dh.time_changed (last_time, ev.time)
                monitor (last_time, my.dh)

            if ev.type == "join":
                my.dh.add_node (ev.id)
            if ev.type == "fail":
                my.dh.fail_node (ev.id)
            if ev.type == "crash":
                my.dh.crash_node (ev.id)
            if ev.type == "insert":
                my.dh.insert_block (ev.id, ev.block, ev.size)
            last_time = ev.time
        monitor (last_time + 1, my.dh)

def file_evgen (fname):
    lineno = 0
    fh = open (fname)
    for l in fh:
        lineno += 1
        # Rudimentary comment parsing
        if l[0] == '#': continue
        a = l.strip ().split ()
        try:
            ev = event (int(a[0]), a[1].lower (), a[2:])
            yield ev
        except Exception, e:
            sys.stderr.write ("Bad event at line %d: %s\n" % (lineno, e))


def print_monitor (t, dh):
    allb = sum (dh.blocks.values ())
    sb   = sum ([n.sent_bytes for n in dh.nodes])
    ub   = sum ([n.bytes      for n in dh.nodes])
    
    print t, "%d nodes;" % len(dh.nodes), 
    print "%s bytes sent;" % size_rounder (sb),
    print "%s bytes put;" % size_rounder (allb),
    print "%s bytes used;" % size_rounder (ub),
    print "%d/%d blocks avail" % (dh.available_blocks (), len (dh.blocks))

def parsable_monitor (t, dh):
    allb = sum (dh.blocks.values ())
    sb   = sum ([n.sent_bytes for n in dh.nodes])
    ub   = sum ([n.bytes      for n in dh.nodes])
    print t, len(dh.nodes), sb, allb, ub, dh.available_blocks (), len (dh.blocks)

if __name__ == '__main__':
    import sys
    import getopt

    def usage ():
	sys.stderr.write ("%s events.txt dhash_fragments 7 14\n" % sys.argv[0])

    # default monitor
    monitor = print_monitor
    try:
        opts, cmdv = getopt.getopt (sys.argv[1:], "m")
    except getopt.GetoptError:
        usage ()
        sys.exit (1)
    for o, a in opts:
        if o == '-m':
            monitor = parsable_monitor
            
    if len(cmdv) < 2:
        usage ()
	sys.exit (1)

    evfile = cmdv[0]
    dtype  = cmdv[1]
    gdh = None
    if (dtype == "fragments"):
	try:
	    dfrags = int (cmdv[2])
	except:
	    dfrags = 3
	try:
	    efrags = int (cmdv[3])
	except:
	    efrags = 2 * dfrags
	gdh = dhash.dhash_fragments (dfrags, efrags)
    elif (dtype == "replica"):
	try:
	    replicas = int (cmdv[2])
	except:
	    replicas = 3
	gdh = dhash.dhash_replica (replicas)
    elif (dtype == "replica_norepair"):
	try:
	    replicas = int (cmdv[2])
	except:
	    replicas = 3
	gdh = dhash.dhash_replica_norepair (replicas)
    else:
	sys.stderr.write ("invalid dhash type\n")
        usage ()
	sys.exit (1)

    sim = simulator (gdh)
    eg = file_evgen (evfile)
    sim.run (eg, monitor)
    
