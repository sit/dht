#!/usr/bin/env python

import os
import sys

debug = 0

def load_reboots (fn):
    """Parse lines like:
    Reboot alice.cs.princeton.edu at 1095157217 up for 509274 down for 1623008 detected after 63607
    Reinstall planetlab8.millennium.berkeley.edu at 1102028035 until 1102031217
    """

    db = {}
    fh = open (fn)
    for line in fh:
	f = line.strip ().split ()
	if f[0] == 'Reboot':
	    host  = f[1]
	    rtime = int(f[3])
	    dtime = rtime - int(f[9])
	elif f[0] == 'Reinstall':
	    host  = f[1]
	    dtime = int(f[3])
	    rtime = int(f[5])
	
	hlist = db.get (host,[])
	hlist.append ((dtime, rtime))
	db[host] = hlist
    return db

def ring_combinations (l, n, seed = 0):
    """Generate all possible overlapping setes of n nodes in a ring from l"""
    import random
    # Simulate a "ring" by randomly permuting all nodes 
    random.seed (seed)
    random.shuffle (l) 
    total = len (l)
    if n > total: n = total
    for i in range(0, total):
	if i+n < total:
	    yield l[i:i+n]
	else:
	    yield l[i:] + l[:(i+n-total)]

def all_combinations (l, n):
    """Generate all possible combinations of n elements selected from l"""
    assert (n > 0)
    def rloop (l, comb, k):
	if k == 0:
	    yield comb
	else:
	    for i in xrange(len(l)):
		newcomb = comb + [l[i]]
		for o in rloop (l[i+1:], newcomb, k - 1):
		    yield o
    return rloop (l, [], n)

# accessor constants
B, E, N = 0, 1, 2
# State to help track simultanoues failures
MAX = long (1) << 31
class simul_state (dict):
    def __init__ (my):
	dict.__init__ (my)
	my.cur_begin = MAX
	my.cur_end   = MAX
    def resetsimul (my):
	my.clear ()
	my.cur_begin = MAX
	my.cur_end   = MAX
    def addsimul (my, fs):
	my[fs[N]] = fs
	if fs[B] < my.cur_begin: my.cur_begin = fs[B]
	if fs[E] < my.cur_end:   my.cur_end   = fs[E]
    def __delitem__ (my, fsn):
	dict.__delitem__ (my, fsn)
	my.cur_begin = min (MAX, map (lambda x: x[B], my.values ()))
	my.cur_end   = min (MAX, map (lambda x: x[E], my.values ()))

# XXX do disk failures matter?
def find_simultaneous (f, nodes, timeout):
    """Iterate over the set of outages (sorted, I presume) for the nodes (via db)
       to see if there are any start times within a timeout window for all of
       the nodes.  The timeout is specified in seconds."""
    cur_simul = simul_state ()
    nnodes = len (nodes)
    answer = 0

    i = 0
    while i < len (f) - nnodes: 
	# Shorthand
	b, e, n = f[i][B], f[i][E], f[i][N]
	# Current time represented by b.
	# Bring nodes that have recovered back online.

	for cs in cur_simul.keys ():
	    if cur_simul[cs][E] < b:
#		if debug: print "Expiring", cs
		del cur_simul[cs]

	# Start looking for simultanaeity
	if len (cur_simul) == 0:
	    cur_simul.addsimul (f[i])
#	    if debug: print "Adding", n, "initial"
	    i += 1
	    continue

	# Failure of same node must happen after
#	assert n not in cur_simul

	# We'll be simultaneous if this starts within timeout of earliest begin
	# And if it starts before the earliest end.
	if b < cur_simul.cur_begin + timeout and b < cur_simul.cur_end:
	    cur_simul.addsimul (f[i])
#	    if debug: print "Adding", n, "simul"
#	elif debug:
#	    print "Skipping", n

	if len (cur_simul) == nnodes:
	    answer += 1
#	    if debug:
#		print "===="
#		print "All down simultaneously:", cur_simul.cur_begin, cur_simul.cur_end
#		print cur_simul.values ()
#		print "===="
	i += 1
    return answer

if __name__ == '__main__':
    if (len (sys.argv) < 3):
	print "Usage:", sys.argv[0], "reboots n t"
	sys.exit (1)


    try:
	seed = int (os.environ['SEED'])
    except:
	seed = 0
    db = load_reboots (sys.argv[1])
    n  = int (sys.argv[2])
    ts = map (int, sys.argv[3:])

    hosts = db.keys ()
    nsimults = {}
    for t in ts: nsimults[t] = []
    for combination in ring_combinations (hosts, n, seed):
	f = []	# list of all failures
	for c in combination:
	    f += map (lambda x: (x[0], x[1], c), db[c])
	f.sort (lambda a, b: cmp (a[0], b[0]))
	    
	for t in ts:
	    nsimult = find_simultaneous (f, combination, t)
	    nsimults[t].append (nsimult)
	if (len(nsimults[ts[0]]) + 1) % 100 == 0:  sys.stderr.write(".")
	if (len(nsimults[ts[0]]) + 1) % 7000 == 0: sys.stderr.write("\n")
    for t in ts:
	nsimults[t].sort ()
	print "Total number of failures", n, "nodes within", t, "seconds:", sum (nsimults[t])
	print "  Fewest simultaneous failures:", min(nsimults[t])
	print "  Most simultaneous failures:", max(nsimults[t])
 	print "  Median:", nsimults[t][len(hosts)/2]
	print "  Average simult failures per combo:", float(sum(nsimults[t]))/len(nsimults[t])
