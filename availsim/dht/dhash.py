from sha import sha
import bisect
from utils import size_rounder
from simulator import event

def between (i, a, b):
    if a < b:
        return a < i and i < b
    if a == b:
        return a == i
    if a > b:
        return i > a or i < b
    
# 1.5 Mbps fixed, with 20% overhead, expressed in bytes.
BANDWIDTH_HACK = int ((1.5 * (1024 * 1024 / 8)) * 0.8)
# 128 Kbps, 20% overhead, in bytes.
BANDWIDTH_HACK = int ((128 * (1024 / 8)) * 0.8)
BANDWIDTH_HACK = int ((56 * (1024 / 8)) * 0.8)
class node:
    """A subsidiary class for Chord/DHash nodes"""
    def __init__ (my, id):
        my.id = id
        my.blocks = {}

	my.cached_blocks = {}
        my.alive = 1
        my.bytes = 0

        my.last_alive = 0
        my.lifetime = 0

	my.bandwidth = BANDWIDTH_HACK
	my.nexttimetosend = -1.0
        
        my.nrpc = 0
        my.sent_bytes = 0
	my.sent_bytes_breakdown = {}
	sbkeys = ['insert', 'join_repair_write', 'join_repair_read',
		  'failure_repair_write', 'failure_repair_read', 'pm']
	for k in sbkeys:
	    my.sent_bytes_breakdown[k] = 0

    def start (my, t):
        my.alive = 1
	my.last_alive = t
    def stop (my, t):
        my.alive = 0
	my.lifetime += t - my.last_alive
    def crash (my, t):
        my.stop (t)
        my.blocks = {}
        my.bytes  = 0

    def store (my, block, size):
	# Dead nodes store no (new) blocks
	if not my.alive:
	    return
        if block not in my.blocks:
            my.blocks[block] = size
            my.bytes += size

    def unstore (my, block):
	print "# DELETE", block, "from", my.id 
	my.bytes -= my.blocks[block]
	del my.blocks[block]

    def sendremote (my, t, size):
	# t is now
	my.nrpc += 1
	my.sent_bytes += size
	delta_t = float(size)/my.bandwidth

	if my.nexttimetosend < t:
	    my.nexttimetosend = float (t)
	my.nexttimetosend += delta_t
	return my.nexttimetosend

    def __cmp__ (my, other):
        return cmp (my.id, other.id)
    def __repr__ (my):
        return "%d" % my.id
    def __str__ (my):
        # return hex(long(my.id)).lower()[2:-1]
        return "%d" % my.id

class chord:
    """A Global View Chord simulator.
    
    This is the base class for various derived classes, providing
    basic "routing" utility functions and keeping global knowledge
    about available nodes.

    The simulator expects to be talking to something that provides
    a process method that takes an event.
    """
    def __init__ (my, args = []):
        my.nodes = []
        my.deadnodes = {}
        my.allnodes = {}

    # Event handling functions
    def process (my, ev):
	newevs = []
	if ev.type == "join":
	    newevs = my.add_node (ev.time, ev.id)
	elif ev.type == "fail":
	    newevs = my.fail_node (ev.time, ev.id)
	elif ev.type == "crash":
	    newevs = my.crash_node (ev.time, ev.id)
	return newevs

    def add_node (my, t, id):
        """Find or create the node with id"""
	try:
	    nnode = my.allnodes[id]
	    if nnode.alive:
		raise RuntimeError, "Duplicate insert of %s" % hex(id).lower ()
	    else:
		# This might be a re-join of some sort
		nnode.start (t)
		del my.deadnodes[id]
		bisect.insort (my.nodes, nnode)
	except KeyError:
            nnode = node (id)
            my.allnodes[id] = nnode
            bisect.insort (my.nodes, nnode)

	return []
    
    def _failure (my, t, id, crash):
	try:
	    n = my.allnodes[id]
	except KeyError:
	    return []
        if n.alive:
            if crash:
                n.crash (t)
            else:
                n.stop (t)
            d = bisect.bisect (my.nodes, n)
            assert my.nodes[d - 1] == n, "Expected %s got %s." % (n, my.nodes[d-1])
            my.nodes.pop (d - 1)
            my.deadnodes[n.id] = n
	return []
        
    def fail_node (my, t, id):
        return my._failure (t, id, 0)

    def crash_node (my, t, id):
        return my._failure (t, id, 1)

    # Utility functions
    dummy = node (0)
    def find_successor_index (my, id):
        # if id in my.nodes, returns the index of id.
	my.dummy.id = id
        n = bisect.bisect_left (my.nodes, my.dummy)
        if n >= len(my.nodes):
            n = 0            
        return n
    def find_predecessor_index (my, id):
	dummy = node (id)
        n = bisect.bisect_left (my.nodes, dummy)
        if n == 0:
            n = len (my.nodes)
        n -= 1
        return n

    def succ (my, o, num = 1):
        """Return successor of o from active nodes. If o is active node,
        returns o. If num not specified, returns immediate successor.
        Otherwise, returns list of num immediate successors."""
        id = o
        if isinstance (o, node): id = o.id
        n = my.find_successor_index (id)
        diff = n + num - len (my.nodes)
        if diff > 0:
            if diff > n: diff = n
            return my.nodes[n:] + my.nodes[:diff]
        return my.nodes[n:n+num]

    def pred (my, o, num = 1):
        """Returns num predecessor's of o, not including o if o in nodes."""
        id = o
        if isinstance (o, node): id = o.id
        n = my.find_predecessor_index (id)
        diff = n - num
        if diff < 0:
            diff += len (my.nodes)
            if diff < n: diff = n
            return my.nodes[diff+1:] + my.nodes[:n+1]
        return my.nodes[diff+1:n+1]

class dhash (chord):
    """Provides a paramterizable insertion and repair of blocks.
    The repair implementation does not delete excess fragments from nodes.
    Nodes may wish to run GC separately."""
    # XXX How long to wait until we start repair? Not instant??

    def __init__ (my, args = []):
	chord.__init__ (my, args)
	my.do_repair = 0
        my.blocks = {}
        my.block_keys = []
	my.available = {}
	for a in args: 
	    if a == 'repair':
		my.do_repair = 1
		my.add_node   = my.add_node_repair
		my.fail_node  = my.fail_node_repair
		my.crash_node = my.crash_node_repair
	    elif a == 'repair++crash':
		my.do_repair = 1
		my.add_node   = my.add_node_repair
		my.crash_node = my.crash_node_repair

    def process (my, ev):
	if ev.type == "insert":
	    return my.insert_block (ev.id, ev.block, ev.size)
	elif ev.type == "copy":
	    return my.copy_block (ev.src_id, ev.id, ev.block, ev.size, ev.desc)
	return chord.process (my, ev)

    def available_blocks (my):
	"""Find how many blocks would be using look_ahead reading."""
	scannable = my.look_ahead ()
        needed = my.read_pieces ()
	k = my.block_keys
	getsucclist = my.succ
	avail = 0
	try:
	    succs = getsucclist (k[0], scannable) 
	    szeroid = succs[0].id
	except IndexError:
	    return 0
	for b in k:
	    extant = 0
	    if b > szeroid:
		succs = getsucclist (b, scannable)
		szeroid = succs[0].id
	    for s in succs:
		if b in s.blocks:
		    extant += 1
	    if extant >= needed:
		avail += 1
	return avail

    # Subclass and redefine these methods to produce more interesting
    # storage behavior.
    def insert_block (my, id, block, size):
        """Basic insertion code to store insert_pieces() pieces on
        successors of key, via id.."""
        if block not in my.blocks:
            my.blocks[block] = size
	    bisect.insort (my.block_keys, block)
        succs = my.succ (block, my.insert_pieces ())
        isz   = my.insert_piece_size (size)
        for s in succs:
            s.store (block, isz)
	my.available[block] = len (succs)
        n = my.allnodes[id]
        n.nrpc += len (succs)
        n.sent_bytes += isz * len (succs)
	n.sent_bytes_breakdown['insert'] += isz * len (succs)
        return []

    def copy_block (my, src, dst, block, size, desc):
	"""."""
	# Typically used for repairs so should already know about this
	assert block in my.blocks
	if src in my.deadnodes or dst in my.deadnodes:
	    return []

	s = my.allnodes[src]
	d = my.allnodes[dst]
	s.sent_bytes_breakdown['%s_repair_write' % desc] += size
	d.store (block, size)

	real_succs = my.succ (block, my.look_ahead ())
	if d in real_succs:
	    my.available[block] += 1
	return []

    def _repair (my, t, an, succs, resp_blocks):
	"""Helper to repair that does real work"""
        desc = "failure"
        if an.alive: desc = "join"
	events = []
	read_pieces = my.read_pieces ()
	min_pieces = my.min_pieces ()
	insert_piece_size = my.insert_piece_size 
        for b in resp_blocks:
            # Check their availability
	    haves = []
	    donthaves = []
            for s in succs:
                if b in s.blocks:
		    haves.append (s)
		else:
		    donthaves.append (s)
	    avail = len (haves)
	    my.available[b] = avail
            if avail == 0:
		# print "# LOST block", b, "after", desc, "of", an, "|", succs
		pass
	    elif avail < min_pieces:
		# print "# REPAIR block", b, "after", desc, "of", an
		needed = min_pieces - avail
                isz = insert_piece_size (my.blocks[b])
		fixer = haves.pop (0)
		# XXX should pick the best fixers? min nextsendtime
		#     successor directs all repairs?
                for s in donthaves:
		    # Round event time up to the next whole unit
		    nt = int (fixer.sendremote (t, isz) + 0.5)
		    events.append (event (nt, "copy", [desc, fixer.id, s.id, b, isz]))
		    needed -= 1
		    if needed <= 0: break
		if b not in fixer.cached_blocks:
		    # Account for bytes needed to reassemble the block.
		    nread = read_pieces
		    for s in haves:
			# the fixer has his own copy
			nread -= 1
			if nread <= 0: break
			s.sent_bytes += isz
			s.sent_bytes_breakdown['%s_repair_read' % desc] += isz
		    fixer.cached_blocks[b] = 1
		    # XXX account for disk used by cache
	return events

    def repair (my, t, affected_node):
	newevs = []
	runlen = min (my.look_ahead (), len (my.nodes))
        preds = my.pred (affected_node, runlen)
        succs = my.succ (affected_node, runlen)
        # succ's does not include affected_node if it is dead.
        span = preds + succs
	k = my.block_keys
	# consider the predecessors who should be doing the repair
        for i in range(1,len(span) - runlen + 1):
            p = span[i - 1]
	    # let them see further than they would have inserted.
            s = span[i:i+runlen]
	    if (p.id <= s[0].id):
		start = bisect.bisect_left (k, p.id)
		stop  = bisect.bisect_right (k, s[0].id)
		r = k[start:stop]
	    else:
		start = bisect.bisect_right (k, p.id)
		stop  = bisect.bisect_left  (k, s[0].id)
		r = k[start:] + k[:stop]
            newevs += my._repair (t, affected_node, s, r)
	return newevs

    # Primary implementations that keep track of what blocks are
    # available incrementally.
    def add_node (my, t, id):
	newevs = chord.add_node (my, t, id)
	n = my.allnodes[id]
	av = my.available
	la = my.look_ahead ()
	getsucclist = my.succ
	for b in n.blocks:
	    # XXX optimize to only call my.succ when it should change
	    real_succs = getsucclist (b, la + 1)
	    if n in real_succs[:-1]:
		av[b] += 1
	    if b in real_succs[-1].blocks:
		av[b] -= 1
	return newevs
    def fail_node (my, t, id):
	try:
	    n = my.allnodes[id]
	    av = my.available
	    la = my.look_ahead ()
	    getsucclist = my.succ
	    for b in n.blocks:
		real_succs = getsucclist (b, la)
		if n in real_succs:
		    av[b] -= 1
	    return chord.fail_node (my, t, id)
	except:
	    return []
    def crash_node (my, t, id):
	try:
	    n = my.allnodes[id]
	    av = my.available
	    la = my.look_ahead ()
	    getsucclist = my.succ
	    for b in n.blocks:
		real_succs = getsucclist (b, la)
		if n in real_succs:
		    av[b] -= 1
	    return chord.crash_node (my, t, id)
	except:
	    return []

    # Alternate implementations that are called if do_repair == 1
    # Block availability is tracked via repair instead.
    def add_node_repair (my, t, id):
	newevs = chord.add_node (my, t, id)
	newevs += my.repair (t, my.allnodes[id])
	return newevs
    def fail_node_repair (my, t, id):
	newevs = chord.fail_node (my, t, id)
	try: 
	    newevs += my.repair (t, my.allnodes[id])
	except KeyError:
	    pass
	return newevs
    def crash_node_repair (my, t, id):
	newevs = chord.crash_node (my, t, id)
	try:
	    newevs += my.repair (t, my.allnodes[id])
	except KeyError:
	    pass
	return newevs
	
    # Subclass and redefine these methods to explore basic changes.
    def min_pieces (my):
        """The minimum number of pieces we need before repairing.
        Probably should be at least as big as read_pieces."""
        return 0
    def read_pieces (my):
        """The number of pieces on different nodes needed to for successful read."""
        return 0
    def look_ahead (my):
	"""Number of nodes ahead to check to see if there are already fragments."""
	return 0
    def insert_pieces (my):
        """The number of pieces to write into the system initially."""
        return 0
    def insert_piece_size (my, whole_size):
        """How big is each piece that gets inserted?"""
        return 0

class dhash_replica (dhash):
    """Simple replication 
    First argument should be number of replicas (defaulting to three).
    If repair desired, add additional 'repair' argument"""
    def __init__ (my, args = []):
        dhash.__init__ (my, args)
	try:
	    my.replicas = int (args[0])
	except:
	    my.replicas = 3

    def min_pieces (my):
        return my.replicas
    def read_pieces (my):
        return 1
    def insert_pieces (my):
        return my.replicas
    def insert_piece_size (my, size):
        return size
    def look_ahead (my):
	return 32

class dhash_fragments (dhash):
    def __init__ (my, args = []):
        dhash.__init__ (my, args)
	try:
	    my.dfrags = int (args[0])
	except:
	    my.dfrags = 3
	try:
	    my.efrags = int (args[1])
	except:
	    my.efrags = 2 * my.dfrags

    def min_pieces (my):
        # Should this by dfrags?
        return my.efrags
    def read_pieces (my):
        return my.dfrags
    def insert_pieces (my):
        return my.efrags
    def look_ahead (my):
	return 3 * my.efrags
    def insert_piece_size (my, size):
        # A vague estimate of overhead from encoding... 2%-ish
        return int (1.02 * (size / my.dfrags))

class dhash_cates (dhash):
    def min_pieces (my):
        return 14
    def read_pieces (my):
        return 7
    def insert_pieces (my):
        return 14
    def look_ahead (my):
	return 16
    def insert_piece_size (my, size):
        # A vague estimate of overhead from encoding... 2%-ish
        return int (1.02 * (size / 7))

    def add_node (my, t, id):
	newevs = dhash.add_node (my, t, id)
	if len (my.nodes) >= 16:
	    newevs += my._pmaint_join (my.allnodes[id])
	return newevs

    def _pmaint_join (my, n):
	succs = my.succ (n, 18)
	preds = my.pred (n, 16)
	nid = n.id
	# Perhaps this new guy has returned with some new blocks
	# that he now should give away to someone else...
	pid = preds[0].id
	lostblocks = filter (lambda x: not between (x, pid, nid), n.blocks)
	# if len(lostblocks): print "# LOST", len(lostblocks), "blocks"
	for b in lostblocks:
	    lsuccs = my.succ (b, 14)
	    for s in lsuccs:
		if b not in s.blocks:
		    assert n != s
		    isz = my.insert_piece_size (my.blocks[b])
		    s.store (b, isz)
		    n.sent_bytes += isz
		    n.sent_bytes_breakdown['pm'] += isz
		    break
	    # Either someone wanted it and they took it, or no one wanted it.
	    # Safe to delete either way 
	    # XXX of course no one wants this... the moment n failed,
	    #     dhash's aggressive repair fixed the low availability.
	    n.unstore (b)

	# Next, fix blocks that are in the wrong place now because
	# this guy appeared; give them to this new guy.
	for s in succs[1:]:
	    lostblocks = filter (lambda x: between (x, pid, nid), s.blocks)
	    if len(lostblocks): print "# LoST", len(lostblocks), "blocks"
	    for b in lostblocks:
		if b not in n.blocks:
		    print "# MOVING", b, "from", s, "to", n
		    isz = my.insert_piece_size (my.blocks[b])
		    n.store (b, isz)
		    s.sent_bytes += isz
		    s.sent_bytes_breakdown['pm'] += isz
		s.unstore (b)

class dhash_oracle (dhash):
    def __init__ (my, args = []):
	# Make sure we don't pass any 'repair' args through.
	# We like our oracular repair code better. :-)
	args = filter (lambda x: x[:6] != 'repair', args)
	dhash.__init__ (my, args)

    def _repair (my, t, an, succs, resp_blocks):
	insert_piece_size = my.insert_piece_size
	blocks = my.blocks
        for b in resp_blocks:
            # Check their availability
	    haves = []
	    donthaves = []
            for s in succs:
                if b in s.blocks:
		    haves.append (s)
		else:
		    donthaves.append (s)
	    avail = len (haves)
	    # I might fail or the only copy might be pushed off soon
	    if avail == 1 and (haves[0] == an or haves[0] == succs[len(succs) - 1]):
		s = donthaves[0]
                isz = insert_piece_size (blocks[b])
		s.store (b, isz)
		haves[0].nrpc += 1
		haves[0].sent_bytes += isz

		if haves[0] == succs[len(succs) - 1]:
		    haves[0].unstore (b)
		    haves[0].sent_bytes_breakdown['join_repair_write'] += isz
		else:
		    haves[0].sent_bytes_breakdown['failure_repair_write'] += isz
	return []

    def add_node (my, t, id):
	newevs = dhash.add_node (my, t, id)
	assert (len (newevs) == 0)
	my.repair (t, my.allnodes[id])
	return []

    def min_pieces (my):
        return 1
    def read_pieces (my):
        return 1
    def insert_pieces (my):
        return 1
    def look_ahead (my):
	return 32
    def insert_piece_size (my, size):
        return size

class durability_oracle (dhash_oracle):
    """
    Only has to repair before a node leaves permanently, taking away
    the last copy.
    """
    def crash_node (my, t, id):
	try:
	    n = my.allnodes[id]
	except KeyError:
	    return []
	my.repair (t, n)
        my._failure (t, id, 1)
	return []

class availability_oracle (durability_oracle):
    """
    Has to repair whenever the last copy is about to disappear,
    even temporarily, in addition to what durability_oracle does.
    """
    def fail_node (my, t, id):
	try:
	    n = my.allnodes[id]
	except KeyError:
	    return []
	my.repair (t, n)
        my._failure (t, id, 0)
	return []

# For automatic use by availsim
known_types = {'chord': chord,
	       'dhash': dhash,
	       'fragments': dhash_fragments,
	       'replica': dhash_replica,
	       'cates': dhash_cates,
	       'replica_durability_oracle': durability_oracle,
	       'replica_availability_oracle': availability_oracle
	       }

if __name__ == '__main__':
    gdh = dhash_replica ()
    gdh.add_node (0, 55)
    gdh.add_node (1, 4)
    gdh.add_node (2, 23)
    gdh.add_node (2, 30)
    gdh.add_node (3, 17)
    gdh.add_node (5, 42)
    gdh.add_node (7, 63)
    try:
        gdh.add_node (8, 4)
        assert 0, "Should not allow duplicate insert!"
    except RuntimeError:
        pass

    # Make sure list is sorted
    start = 0
    for n in gdh.nodes:
        assert n.id > start
        start = n.id

    # Do blocks go to the right place?
    def test_insert (blockid, expected):
	events = gdh.insert_block (4, blockid, 8192)
	assert blockid in gdh.allnodes[expected].blocks
    test_insert (73, 4)
    test_insert (3, 4)
    test_insert (4, 4)
    test_insert (20, 23)
    test_insert (56, 63)

    assert gdh.find_predecessor_index (4) == 6
    assert gdh.find_predecessor_index (66) == 6
    assert gdh.find_predecessor_index (10) == 0
    assert gdh.find_predecessor_index (25) == 2

    assert len(gdh.succ(0, 3)) == 3
    assert len(gdh.succ(63, 3)) == 3
    assert len(gdh.succ(24, 3)) == 3
    assert len(gdh.succ(35, 3)) == 3

    assert gdh.pred (5)[0].id == 4
    assert gdh.pred (31)[0].id == 30
    assert gdh.pred (30)[0].id == 23
    assert gdh.pred (3)[0].id == 63
    assert len(gdh.pred(5, 3)) == 3


