#!/usr/bin/env python

from __future__ import generators
import sys

from utils import make_chordID, str2chordID
import dhash

def get_node_id (args):
    if len(args) == 1:
        try:
            return long(args[0])
        except:
            return str2chordID (args[0])
    elif len(args) == 2:
        return dhash.make_chordID (args[0], int(args[1]), 0)
    elif len(args) == 3:
        return dhash.make_chordID (args[0], int(args[1]), int(args[2]))
    else:
        raise SyntaxError, "Bad ID specification"
        
class event:
    def ev_getnode (my, args):
        id = get_node_id (args)
        my.id = id

    def ev_getblock (my, args):
        try:
            id = long (args[0])
        except:
            id = str2chordID (args[0])
        my.block = id
        try:
            my.size = int(args[1])
        except:
            my.size = 8192
    
    def __init__ (my, time, type, args):
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
        etypes[type] (args)
        
    def __str__ (my):
        return "%ld %s" % (time, type)

class simulator:
    def __init__ (my, dht):
        my.dh = dht
        
    def run (my, evgen, monitor):
        last_time = -1
        for ev in evgen:
            assert last_time <= ev.time, "Woah! Time can't go backwards. Bye."
            # Call the monitor after the time changes.
            if last_time != ev.time:
                monitor (ev.time, my.dh)

            if ev.type == "join":
                my.dh.add_node (ev.id)
            if ev.type == "fail":
                my.dh.fail_node (ev.id)
            if ev.type == "crash":
                my.dh.crash_node (ev.id)
            if ev.type == "insert":
                my.dh.insert_block (ev.block, ev.size)
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


def avail_calculator (t, dh):
    print t, "%d nodes" % len(dh.nodes), dh.availability_summary ()
    # should find some way to optimally know what changed since
    # last t, and directly calculate its impact on availability...


if __name__ == '__main__':
    import sys
    sim = simulator (dhash.dhash_fragments (3, 6))
    sim.run (file_evgen(sys.argv[1]), avail_calculator)
    
