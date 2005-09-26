
"""
An educated guess at what Total Recall might be doing,
at least in the implementation as described in:

R. Bhagwan, K. Tati, Y. Cheng, S. Savage, and G. M. Voelker.
Total recall: System support for automated availability management.
In Proceedings of the 1st ACM/Usenix Symposium on NSDI, 2004.

http://ramp.ucsd.edu/projects/recall/
http://www.usenix.org/events/nsdi04/tech/bhagwan.html

And source code provided by the authors.
"""

import math
from random import sample	# python 2.3 and later
import random

from utils import random_id
from chord import chord
from simulator import event

def _sigmas (avail):
    # Presumably this comes from the normal approx of binom distrib.
    # These particular numbers taken from RSM/FileCache-WriteOp.C
    # in the function CalculateRedundancy.
    if avail <= 0.8:
	return 0.841621
    if avail <= 0.9:
	return 1.28155
    if avail <= 0.99:
	return 2.326235
    return 3.09023

def _calc_stretch_factor (desired_avail, nblocks):
    # Mean host availability.  Formulas in Section 3.2.
    # Their prototype implementation uses fixed mu_h of 0.5.
    # See RSM/Filecache-WriteOp.C's MinHostAvailability implementation.
    # However, the paper suggests that they used a value of 0.65 for
    # their simulations (Section 6.2).
    # For mu_h = 0.65, desired_avail = 0.99, nblocks = 4, you get 1.866, or 2.
    mu_h = 0.65
    muhh = mu_h * (1.0 - mu_h)
    # For desired_avail = 0.99, nblocks = 4, mu_h = 0.65
    k = _sigmas (desired_avail)
    k2 = k*k
    t = muhh / nblocks
    t2 = k * math.sqrt (t) + \
	     math.sqrt (k2 * t + 4.0 * mu_h)/(2 * mu_h)
    return math.ceil (t2)

class totalrecall_base (chord):
    """Common parts of lazy TR for both replication and fragmentation.
    We would like perhaps to inherit from DHash but really we only
    care about the interface."""
    def __init__ (my, args):
	my.placement = my._random_placement
	# short and long term redundancy factors
	try:
	    my.shortt = int (args.pop (0))
	except:
	    my.shortt = 2
	try:
	    my.longt = int (args.pop (0))
	except:
	    my.longt = 4
	try:
	    if args[0] == 'succplace':
		my.placement = my._successor_placement
		args.pop (0)
	except IndexError:
	    pass
	chord.__init__ (my, args)

	# Mapping from blocks to size
        my.blocks = {}
	# Mapping for blocks to hosts holding them
	# XXX should really be stored with eager replication
	#     on successor nodes, but this is easier.
	my.inodes = {}
	# Number of available copies of each block
	my.available = {}
	# block -> (time of last unavailability) mapping 
	my.unavailable_time = {}
	# The total number of seconds that blocks are unavailable.
	my.total_unavailability = 0

	# For debugging
	# random.seed (0)

    def process (my, ev):
	now = ev.time
	uat = my.unavailable_time
	for b in uat:
	    my.total_unavailability += now - uat[b]
	    uat[b] = now
	if ev.type == "insert":
	    return my.insert_block (ev.id, ev.block, ev.size)
	elif ev.type == "copy":
	    return my.copy_block (ev.time, ev.src_id, ev.id, ev.src_time, ev.block, ev.size, ev.desc)
	return chord.process (my, ev)

    def _available_blocks_check (my):
	needed = my.read_pieces ()
	available = my.available
	extants = []
	for b in my.inodes:
	    hosts = my.inodes[b]
	    extants.append (len([n for n in hosts if n.alive and b in n.blocks]))
	    # XXX check to see if our book keeping is correct
	    assert extants[-1] == available[b], "%d %d %s" % (extants[-1], available[b], hosts)
	avail = len ([b for b in extants if b >= needed])
	return avail, extants

    def available_blocks (my):
	"""Number of readable blocks"""
	needed = my.read_pieces ()
	counts = my.available.values ()
	extant = [x for x in counts if x >= needed]
	return len (extant), counts

    def _random_placement (my, b, n):
	"""chooses n random nodes to hold block b"""
	options = my.nodes
	try:
	    excl = my.inodes[b]
	    options = [o for o in options if o not in excl]
	except KeyError:
	    pass
	return sample (options, n)

    def _successor_placement (my, b, n):
	"""chooses n successor nodes to hold block b"""
	options = my.succ (b, 2*my.longt)
	try:
	    excl = my.inodes[b]
	    options = [o for o in options if o not in excl]
	except KeyError:
	    pass
	return options[:n]

    def insert_block (my, id, block, size):
	# 1. Figure out desired redundancy of block
	# 2. Figure out how many things to write and how big they are.
	# 3. Write them on random nodes.
	# 4. Store the list of randomness somewhere.
	#    XXX just in mem now; add inode-thingies later.
	#    We will default to 5 copies of inode (RSM.C).
        if block not in my.blocks:
            my.blocks[block] = size
	(ninsert, isz) = my.insert_pieces (size) 
	insnodes = my.placement (block, ninsert)
	# insnodes will be ninsert unique nodes
	for i in insnodes:
	    i.store (block, isz)
	my.inodes[block] = insnodes
	my.available[block] = len (insnodes)
        n = my.allnodes[id]
        n.nrpc += ninsert
        n.sent_bytes += isz * ninsert
	n.sent_bytes_breakdown['insert'] += isz * ninsert

    def copy_block (my, t, src, dst, lastsrc, block, size, desc):
	# Typically used for repairs so should already know about this
	assert block in my.blocks
	if src in my.deadnodes or dst in my.deadnodes:
	    return None
	s = my.allnodes[src]
	if s.last_alive != lastsrc:
	    print "# Source failed to copy", block
	    return None
	s.nrpc += 1
	s.sent_bytes += size
	s.sent_bytes_breakdown['%s_repair_write' % desc] += size

	d = my.allnodes[dst]
	needed = my.read_pieces ()
	if block not in d.blocks and d in my.inodes[block]:
	    d.store (block, size)
	    my.available[block] += 1
	    if my.available[block] == needed:
		del my.unavailable_time[block]
	return None

    def _update_inode (my, b, livenodes, newnodes):
	my.inodes[b] = livenodes + newnodes

    def repair (my, t, blocks):
	# The blocks that id was storing are owned by lots of
	# people.  Each one of them needs to figure out that
	# this guy failed and do something about it.
	events = []
	needed = my.read_pieces ()
	for b in blocks:
	    # 1. Partition the nodes in the inodes into live/dead to
	    #    calculate available redundancy factor, $f$.
	    # 2. Produce new redundancy if $f$ < short term, produce
	    #    enough long term redundancy and store on _new_
	    #    random nodes.  No point in remembering the old ones
	    #    because they haven gone for a while.  Maybe see Sec6.2.
	    #    XXX the failure that triggered this repair though
	    #        may be short term, but too bad, we can't tell.
	    livenodes, deadnodes = [], []
	    rfactor = 0.0
	    hosts = my.inodes[b]
	    for n in hosts:
		if n.alive and b in n.blocks:
		    rfactor += n.blocks[b]
		    livenodes.append (n)
		else:
		    deadnodes.append (n)
	    my.available[b] = len (livenodes)
	    if len (livenodes) < needed:
		if b not in my.unavailable_time:
		    my.unavailable_time[b] = t
		    # print "# LOST", b
		continue
	    rfactor /= my.blocks[b]
	    if rfactor < my.shortt:
		(ninsert, isz) = my.insert_pieces (my.blocks[b])
		ninsert -= len (livenodes)
		newnodes = my.placement (b, ninsert)
		# Who fixes?  Someone with a copy, preferably.
		fixer = livenodes[0]
		for i in livenodes:
		    if b in i.cached_blocks:
			fixer = i
			break
		# Send to these guys as soon as we can
		for s in newnodes:
		    nt = int (fixer.sendremote (t, isz) + 0.5)
		    events.append (event (nt, "copy", 
			["failure", fixer.id, fixer.last_alive, s.id, b, isz]))
		    if b in s.blocks:
			# already there!
			my.available[b] += 1
		if b not in fixer.cached_blocks:
		    # Account for bytes needed to reassemble the block
		    # XXX but not for the _time_
		    nread = my.read_pieces ()
		    for s in livenodes:
			# the fixer has his own piece
			nread -= 1
			if nread <= 0: break
			s.sent_bytes += isz
			s.sent_bytes_breakdown['failure_repair_read'] += isz
		    fixer.cached_blocks[b] = 1
		    # XXX account for disk used by cache
		my._update_inode (b, livenodes, newnodes)
	return events

    # Because TR tracks explicitly the nodes that hold
    # a block, joins never cause any action.  Only failures.
    # XXX (not strictly true; joins require eager replication of inodes.)
    def add_node (my, t, id):
	chord.add_node (my, t, id)
	# Track some statistics about block availability
	n = my.allnodes[id]
	inodes = my.inodes
	avail  = my.available
	uat    = my.unavailable_time
	needed = my.read_pieces ()
	for b in n.blocks: 
	    if n in inodes[b]:
		avail[b] += 1
		if avail[b] == needed:
		    del uat[b]
	    else:
		# print "#", n, "not in inodes for" ,b,
		# print "but they are:", inodes[b]
		pass
	
    def fail_node (my, t, id):
	try:
	    n = my.allnodes[id]
	except KeyError:
	    return
	newevs = chord.fail_node (my, t, id)
	# Failure leaves blocks on disk
	evs = my.repair (t, n.blocks.keys ())
	if newevs is not None:
	    newevs += evs
	else:
	    newevs = evs
	return newevs
    def crash_node (my, t, id):
	try:
	    n = my.allnodes[id]
	except KeyError:
	    return
	# Must make a copy
	blocks = n.blocks.keys ()
	newevs = chord.crash_node (my, t, id)
	evs = my.repair (t, blocks)
	if newevs is not None:
	    newevs += evs
	else:
	    newevs = evs
	return newevs

    def insert_pieces (my, size):
	"""Number to insert, how big each is"""
	return 0, 0
    def read_pieces (my):
	"""Number needed to reconstruct"""
	return 0

class totalrecall_lazy_replica (totalrecall_base):
    """Do lazy repair but for replicas.
    Section 4.3 is the key here."""
    def insert_pieces (my, size):
	return my.longt, size
    def read_pieces (my):
	return 1

class dhash_replica (totalrecall_base):
    """Do Sostenuto repair for replicas."""
    def __init__ (my, args = []):
	try:
	    my.replicas = int (args[0])
	except:
	    my.replicas = 3
	totalrecall_base.__init__ (my, args)
	my.shortt = my.replicas
	my.longt  = my.replicas

    def _update_inode (my, b, livenodes, newnodes):
	my.inodes[b] += newnodes

    def insert_pieces (my, size):
	return my.replicas, size
    def read_pieces (my):
	return 1

class dhash_replica_oracle (dhash_replica):
    """Just act the same but we know we need do nothing on transient"""
    def fail_node (my, t, id):
	try:
	    n = my.allnodes[id]
	except KeyError:
	    return
	newevs = chord.fail_node (my, t, id)
	return newevs

class totalrecall_lazy_fragments (totalrecall_base):
    """Should read the paper and figure how many fragments it makes"""
    def insert_pieces (my, size):
	assert 0
	# Totally bogus.
	npieces = 0
	return npieces, size
    def read_pieces (my):
	# Totally bogus.
	return 0

# class totalrecall_crap (totalrecall_base):
#     """This might be more realistic but it isn't complete!"""
#    def _num_blocks (my, l):
#	return max (4, (l + 32767)/32768)
#     # XXX Really should just do eager replication for < 32k.
#     def insert_block (my, id, block, size):
# 	nblocks = my._num_blocks (size)
# 	nwblocks = nblocks * my._calc_stretch_factor (0.999, nblocks)
# 	# This is basically 12, for small blocks...
# 	# But the long-term redundancy factor is 4.  Where is the
# 	# long term factor used?  Code says:
# 	#   if total_up / (nhosts * sf) < threshold: repair
# 	# where threshold is hard-coded as 1.9, and nhosts is
# 	# the same as wblocks....
# 	# See RSM/FileCache-WriteOp.C:1240 and RSM/AvailabilityMonitor.C:142
# 
# 	inode_size = 2048
# 	# contains list of nodes, 6 bytes * wblocks,
# 	# list of all blocks stored on each node, etc.
# 
# 	frag_size = (size + nblocks - 1) / nblocks
# 	wblocks = [random_id () for i in range(nwblocks)]
# 	# Simulate erasure coding by making up nwblocks block ids.
# 
#     def repair (my, t, affected_node):
# 	# XXX need to know who might need to repair this block.
# 	pass
# 
#     def available_blocks (my):
# 	# XXX need to iterate over all blocks --- find its successor,
# 	#     and figure out where the actual data is stored, and
# 	#     check how many are still up.
# 	return 0
