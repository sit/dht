import bisect
from utils import between
from simulator import event
from chord import chord

class dhash (chord):
    """Provides a paramterizable insertion and repair of blocks.
    The repair implementation does not delete excess fragments from nodes.
    Nodes may wish to run GC separately."""
    # XXX How long to wait until we start repair? Not instant??

    def __init__ (my, args = []):
	chord.__init__ (my, args)
	my.do_repair = 0
	# Sizes of all blocks
        my.blocks = {}
	# Sorted list of blocks.keys ()
        my.block_keys = []
	# Incrementally maintained mapping from blocks->num of copies
	# in look_ahead () of successor.
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
	    return my.copy_block (ev.src_id, ev.id, ev.src_time, ev.block, ev.size, ev.desc)
	return chord.process (my, ev)

    def _extant_counts_check (my):
	"""How many copies of each block are there?"""
	blocks = {}
	for n in my.nodes:
	    for b in n.blocks:
		blocks[b] = blocks.get (b, 0) + 1
	extant = blocks.values ()
	# assert blocks == my.available # same keys and same values???
	return extant
    def _available_blocks_check (my):
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
	assert my.available_blocks () == avail
	return avail

    def available_blocks (my):
	"""Number of readable blocks"""
	needed = my.read_pieces ()
	counts = my.available.values ()
	extant = [x for x in counts if x >= needed]
	return len (extant), counts

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
        return None

    def copy_block (my, src, dst, lastsrc, block, size, desc):
	"""."""
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
	real_succs = my.succ (block, my.look_ahead ())
	if block not in d.blocks and d in real_succs:
	    d.store (block, size)
	    my.available[block] += 1
	return None

    def _repair_join (my, t, an, succs, resp_blocks):
	if an == succs[-1]:
	    # Blocks that someone else is responsible for
	    # should go back to that person because the next
	    # join might push me out of place.
	    # XXX perhaps should only do this when we're actually
	    #     out of place, a la pmaint.
	    for b in resp_blocks:
		try:
		    # If not stored on an, raise KeyError
		    sz = an.blocks[b]
		except KeyError:
		    continue
		for s in succs:
		    if b not in s.blocks:
			nt = int (an.sendremote (t, sz) + 0.5)
			# XXX should move fragments, or
			# possibly reconstruct and make a new one.
			events.append (event (nt, "copy",
			    ["pmaint", an.id, s.id, b, sz]))
			break

    def _repair_fail (my, t, an, succs, resp_blocks):
	"""Helper to repair_fail that does real work"""
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
		print "# LOST block", b, "after failure of", an, "|", succs
		pass
	    elif avail < min_pieces:
		# print "# REPAIR block", b, "after failure of", an
		needed = min_pieces - avail
                isz = insert_piece_size (my.blocks[b])
		fixer = haves.pop (0)
		# XXX should pick the best fixers? min nextsendtime
		#     successor directs all repairs?
                for s in donthaves:
		    # Round event time up to the next whole unit
		    nt = int (fixer.sendremote (t, isz) + 0.5)
		    events.append (event (nt, "copy",
			["failure", fixer.id, fixer.last_alive, s.id, b, isz]))
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
			s.sent_bytes_breakdown['failure_repair_read'] += isz
		    fixer.cached_blocks[b] = 1
		    # XXX account for disk used by cache
	return events

    def repair_pmaint (my, t, an):
	"""aka partition maintenance"""
	events = []
	runlen = min (my.look_ahead ()+ 1, len (my.nodes))
	getsucclist = my.succ
	for b in an.blocks:
	    # Need to call getsucclist for n == an, since an
	    # might be really far out of place or really old,
	    # but for the other nodes, it will be sort of
	    # repeated overlapping so more of a waste of time.
	    succs = getsucclist (b, runlen)
	    if an not in succs:
		# We're not in the successor list so pmaint
		for s in succs:
		    if b not in s.blocks:
			isz = an.blocks[b]
			nt = int (n.sendremote (t, isz) + 0.5)
			# XXX should move fragments, or
			# possibly reconstruct and make a new one.
			events.append (event (nt, "copy",
			    ["pmaint", an.id, s.id, b, isz]))
			break
	return events

    def repair (my, t, affected_node):
	newevs = []
	runlen = min (my.look_ahead (), len (my.nodes))
        preds = my.pred (affected_node, runlen)
        succs = my.succ (affected_node, runlen)
	if affected_node.alive:
	    newevs = my.repair_pmaint (t, affected_node)
	    repair = my._repair_join
	else:
	    repair = my._repair_fail
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
	    evs = repair (t, affected_node, s, r)
	    if evs is not None:
		newevs += evs
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
	    pass
	return None
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
	    pass
	return None

    # Alternate implementations that are called if do_repair == 1
    # Block availability is tracked via repair instead.
    def add_node_repair (my, t, id):
	newevs = chord.add_node (my, t, id)
	evs = my.repair (t, my.allnodes[id])
	if newevs is not None:
	    newevs += evs
	else:
	    newevs = evs
	return newevs
    def fail_node_repair (my, t, id):
	newevs = chord.fail_node (my, t, id)
	try: 
	    evs = my.repair (t, my.allnodes[id])
	    if newevs is not None:
		newevs += evs
	    else:
		newevs = evs
	except KeyError:
	    pass
	return newevs
    def crash_node_repair (my, t, id):
	newevs = chord.crash_node (my, t, id)
	try:
	    evs = my.repair (t, my.allnodes[id])
	    if newevs is not None:
		newevs += evs
	    else:
		newevs = evs
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
	return 16

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
	    evs = my._pmaint_join (t, my.allnodes[id])
	    if newevs is not None:
		newevs += evs
	    else:
		newevs = evs
	return newevs

    def _repair_join_check (my, t, n, b, succs):
	if n not in succs:
	    # We're not in the successor list so pmaint
	    for s in succs:
		if b not in s.blocks:
		    sz = n.blocks[b]
		    nt = int (n.sendremote (t, sz) + 0.5)
		    # XXX should move fragments, or
		    # possibly reconstruct and make a new one.
		    return event (nt, "copy",
			["pmaint", n.id, s.id, b, sz])

    def _pmaint_join_XXX (my, t, id):
	"""aka partition maintenance"""
	events = []
	runlen = min (my.look_ahead () + 1, len (my.nodes))
	getsucclist = my.succ
	# Fix blocks that we're no longer in the succlist for.
	for b in n.blocks:
	    # Need to call getsucclist for n == an, since an
	    # might be really far out of place or really old,
	    succs = getsucclist (b, runlen)
	    e = my._repair_join_check (t, n, b, succs)
	    if e: events.append (e)
	ansucclist = getsucclist (an, runlen)
	assert ansucclist[0] == an
	anpredlist = my.pred (an, runlen - 1)
	neighborhood = anpredlist + ansucclist
	assert runlen == len (anpredlist)
	# Index of an's successor
	#  i = len (anpredlist) + 1
	# Each of an's successors has just been bumped off 
	# a successor list, so they'd better do some moving.
	for i in range (runlen + 1, len(neighborhood)):
	    pid = neighborhood[i - runlen - 1].id
	    sid = neighborhood[i - 1].id
	    for b in neighborhood[i].blocks:
		if not between (b, pid, sid):
		    isz = n.blocks[b]
		    # We're not in the successor list so pmaint
		    for s in neighborhood[0]: # XXX BOGUS!!!
			if b not in s.blocks:
			    nt = int (n.sendremote (t, isz) + 0.5)
			    # XXX should move fragments, or
			    # possibly reconstruct and make a new one.
			    events.append (event (nt, "copy",
				["pmaint", n.id, s.id, b, isz]))
			    break
	return events
      
    def _pmaint_join (my, t, n):
	events = []
	preds = my.pred (n, 16)
	nid = n.id
	# Perhaps this new guy has returned with some new blocks
	# that he now should give away to someone else...
	pid = preds[0].id
	lostblocks = [b for b in n.blocks if not between (b, pid, nid)]
	# if len(lostblocks): print "# LOST", len(lostblocks), "blocks"
	for b in lostblocks:
	    lsuccs = my.succ (b, 14)
	    for s in lsuccs:
		if b not in s.blocks:
		    assert n != s
		    isz = my.insert_piece_size (my.blocks[b])
		    # Round event time up to the next whole unit
		    nt = int (n.sendremote (t, isz) + 0.5)
		    events.append (event (nt, "copy", ["pmaint", nid, s.id, b, isz]))
		    break
	    # Either someone wanted it and they took it, or no one wanted it.
	    # Safe to delete either way 
	    n.unstore (b)

	# Next, fix blocks that are in the wrong place now because
	# this guy appeared; give them to this new guy.
	succs = my.succ (n, 18)
	for s in succs[1:]:
	    lostblocks = [b for b in s.blocks if between (b, pid, nid)]
	    if len(lostblocks): print "# LoST", len(lostblocks), "blocks"
	    for b in lostblocks:
		if b not in n.blocks:
		    print "# MOVING", b, "from", s, "to", n
		    isz = my.insert_piece_size (my.blocks[b])
		    n.store (b, isz)
		    s.sent_bytes += isz
		    s.sent_bytes_breakdown['pmaint_repair_write'] += isz
		s.unstore (b)
	return events
