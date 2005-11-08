#!/usr/bin/env python

from __future__ import generators	# only 2.2 or newer
import sys

from utils import size_rounder
import simulator
import dht

do_spread = 0

def file_evgen (fname):
    lineno = 0
    fh = open (fname)
    for l in fh:
        lineno += 1
        # Rudimentary comment parsing
        if l[0] == '#': continue
        a = l.strip ().split ()
        try:
            ev = simulator.event (int(a[0]), a[1].lower (), a[2:])
            yield ev
        except Exception, e:
            sys.stderr.write ("Bad event at line %d: %s\n" % (lineno, e))

def calc_spread (dh, stats):
    """Helper function for _monitor to calculate how far apart blocks get.
       Adds spread_min, spread_max, and spread_avg keys to stats table.
    """
    ssum = 0
    smin = 64
    smax = -1
    for b in dh.blocks:
        succs = dh.succ (b, 2*dh.look_ahead())
        found = 0
        examined = 0
        for s in succs:
            examined = examined + 1
            if b in s.blocks:
                found = found + 1
                if (found == dh.read_pieces()):
                    break
        ssum += examined
        if examined < smin: smin = examined
        if examined > smax: smax = examined
    stats['spread_min'] = smin
    if (len(dh.blocks) > 0): stats['spread_avg'] = ssum/len(dh.blocks)
    else: stats['spread_avg'] = 0
    stats['spread_max'] = smax
    
sbkeys = ['insert', 'join_repair_write', 'join_repair_read',
	  'failure_repair_write', 'failure_repair_read', 'pmaint_repair_write']
def _monitor (dh):
    stats = {}
    allnodes = dh.allnodes.values ()

    stats['usable_bytes'] = sum (dh.blocks.values ())
    stats['sent_bytes']   = sum ([n.sent_bytes for n in allnodes])
    stats['disk_bytes']   = sum ([n.bytes      for n in allnodes])
    stats['avail_bytes']  = sum ([n.bytes      for n in dh.nodes])

    #bdist = [len(n.blocks) for n in dh.nodes]
    #bdist.sort ()
    #ab = sum(bdist) / float (len(dh.nodes))
    #sys.stderr.write ("%5.2f blocks/node on avg; max = %d, med = %d, min = %d (%d)\n" % (ab, max(bdist), bdist[len(bdist)/2], min(bdist), bdist.count(min(bdist))))

    for k in sbkeys:
	stats['sent_bytes::%s' % k] = \
		sum ([n.sent_bytes_breakdown.get (k, 0) for n in allnodes])

    stats['avail_blocks'], extant = dh.available_blocks ()
    stats['total_unavailability'] = dh.total_unavailability

    try: 
	avg = sum (extant, 0.0) / len (extant)
	minimum = min (extant)
	maximum = max (extant)
    except:
	avg, minimum, maximum = 0, 0, 0
    stats['extant_avg'] = avg
    stats['extant_min'] = minimum
    stats['extant_max'] = maximum

    if do_spread:
	calc_spread (dh, stats)
    return stats

def print_monitor (t, dh):
    s = _monitor (dh)

    print "%4d" % t, "%4d nodes;" % len(dh.nodes),
    print "%sB sent;" % size_rounder (s['sent_bytes']),
    print "%sB put;" % size_rounder (s['usable_bytes']),
    print "%sB avail;" % size_rounder (s['avail_bytes']),
    print "%sB stored;" % size_rounder (s['disk_bytes']),
    print "%d/%5.2f/%d extant;" % (s['extant_min'], s['extant_avg'], s['extant_max']),
    print "%d/%d blocks avail" % (s['avail_blocks'], len (dh.blocks))
    if do_spread:
	print "%d/%d avg %5.2f block spread" % (s['spread_min'],
						s['spread_max'],
						s['spread_avg'])
    for k in sbkeys:
	print "%sB sent[%s];" % (size_rounder(s['sent_bytes::%s' % k]), k)

def parsable_monitor (t, dh):
    print t, len(dh.nodes), 
    s = _monitor (dh)

    print ' '.join(["%d" % s[k] for k in ['sent_bytes','usable_bytes','avail_bytes','disk_bytes']]),
    print s['extant_min'], "%5.2f" % s['extant_avg'], s['extant_max'],
    print s['avail_blocks'], len (dh.blocks),
    for k in sbkeys:
	print "%d" % s['sent_bytes::%s' % k],
    print s['total_unavailability']

def usage ():
    sys.stderr.write ("%s [-b bandwidth] [-c] [-i int] [-p port] [-m] [-s] events.txt type args\n" % sys.argv[0])
    sys.stderr.write ("where type is:\n")
    a = dht.known_types.keys ()
    a.sort ()
    for t in a:
	sys.stderr.write ("\t%s\n" % t)

if __name__ == '__main__':
    import getopt
    # no threads or signals really
    sys.setcheckinterval (10000000)

    # default monitor, every 12 hours
    monitor = print_monitor
    monint  = 12 * 60 * 60
    dump_bw_cdf = 0
    try:
	opts, cmdv = getopt.getopt (sys.argv[1:], "b:ci:mp:s")
    except getopt.GetoptError:
        usage ()
        sys.exit (1)
    for o, a in opts:
	if o == '-b':
	    dht.node.BANDWIDTH_HACK = int (a)
	elif o == '-c':
	    dump_bw_cdf = 1
	elif o == '-i':
	    monint = int (a)
        elif o == '-m':
            monitor = parsable_monitor
	elif o == '-p':
	    simulator.default_port = int (a)
	elif o == '-s':
	    do_spread = 1
            
    if len(cmdv) < 2:
        usage ()
	sys.exit (1)

    print "# bw =", dht.node.BANDWIDTH_HACK
    print "# args =", cmdv
    print "# port =", simulator.default_port
    evfile = cmdv[0]
    dtype  = cmdv[1]
    gdh = None
    try:
	dhashclass = dht.known_types[dtype]
	gdh = dhashclass (cmdv[2:])
    except KeyError, e:
	sys.stderr.write ("invalid dhash type\n")
        usage ()
	sys.exit (1)

    sim = simulator.simulator (gdh)
    evfh = open (evfile)
    eg = simulator.event_generator (evfh)
    sim.run (eg, monitor, monint)

    if dump_bw_cdf:
	print "###### Per-node sent-bytes (node lifetime stored_bytes sent_bytes",
	print " ".join (sbkeys)
	for n in gdh.allnodes:
	    v = gdh.allnodes[n]
	    print "#B", n, v.lifetime, v.bytes, v.sent_bytes,
	    for k in sbkeys:
		print v.sent_bytes_breakdown[k],
	    print
