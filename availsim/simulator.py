from __future__ import generators	# only 2.2 or newer
from heapq import heappush, heappop	# only 2.3 or newer
import sys

from utils import str2chordID

def get_node_id (args):
    try:
	return long(args)
    except:
	try:
	    return str2chordID (args)
	except ValueError, e:
	    raise SyntaxError, "Bad ID specification", e
        
class event:
    def ev_getnode (my, args):
        my.id = get_node_id (args[0])

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

    def ev_getcopy (my, args):
	my.desc = args.pop (0)
	my.src_id = get_node_id (args.pop (0))
	my.src_time = args.pop (0)
	my.ev_getblock (args)
    
    def __init__ (my, time, type, *args):
        my.time = time
        my.type = type
                
        etypes = {
            'join'  : my.ev_getnode,
            'fail'  : my.ev_getnode,
            'crash' : my.ev_getnode,
            'insert': my.ev_getblock,
	    'copy'  : my.ev_getcopy,
        }
        if type not in etypes:
            raise TypeError, "Unknown event type"
	apply (etypes[type], args)
        
    def __str__ (my):
        return "%ld %s" % (my.time, my.type)

    def __cmp__ (my, other):
	return cmp (my.time, other.time)

class event_generator:
    def __init__ (my, fh = None):
	if fh:
	    my.fh = fh
	my.localev = []

    def add (my, e):
	# e is an event
	heappush (my.localev, e)

    def __iter__ (my):
	lineno = 0
	ev = None
	for l in my.fh:
	    lineno += 1
	    # Rudimentary comment parsing
	    if l[0] == '#': continue
	    a = l.strip ().split ()
	    try:
		ev = event (int(a[0]), a[1].lower (), a[2:])
		while len (my.localev) > 0 and my.localev[0] < ev:
		    yield heappop (my.localev)
		yield ev
	    except Exception, e:
		sys.stderr.write ("Bad event at line %d: %s\n" % (lineno, e))
	while len (my.localev) > 0:
	    yield heappop (my.localev)

class simulator:
    def __init__ (my, dht):
        my.dh = dht
        
    def run (my, evgen, monitor, monint):
        last_time = 0
	do_monitor = 0
	next_monitor_time = monint
        for ev in evgen:
            assert last_time <= ev.time, "Woah! Time can't go backwards %d > %d." % (last_time, ev.time)
            if last_time != ev.time:
		# Call monitor periodically
		if last_time > next_monitor_time or do_monitor:
		    monitor (last_time, my.dh)
		    next_monitor_time += monint
		do_monitor = 0

	    # DHash object knows how to process different events.
	    # It may ask us to pass it back events at some point
	    # in the future too.
	    newevs = my.dh.process (ev)
	    if newevs is not None:
		for newev in newevs:
		    evgen.add (newev)

	    # Be sure to check what happened after an interesting event
	    if ev.type in ["join", "fail", "crash"]: 
		do_monitor = 1
            last_time = ev.time

        monitor (last_time + 1, my.dh)
