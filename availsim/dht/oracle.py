from chord import chord
from dhash import dhash

class dhash_oracle (dhash):
    def __init__ (my, args = []):
	# Make sure we don't pass any 'repair' args through.
	# We like our oracular repair code better. :-)
	args = filter (lambda x: x[:6] != 'repair', args)
	dhash.__init__ (my, args)

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
		succs[0].store (b, sz)
		an.nrpc += 1
		an.sent_bytes += sz
		an.sent_bytes_breakdown['join_repair_write'] += sz
		an.unstore (b)
	return 

    def _repair_fail (my, t, an, succs, resp_blocks):
	insert_piece_size = my.insert_piece_size
	blocks = my.blocks
	# an failed; better move its blocks where they should be
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

	    if avail == 0:
		s = donthaves[0]
		# We're an oracle so we can pretend we have
		# the block, even if the disk crashed.
                isz = insert_piece_size (blocks[b])
		s.store (b, isz)
		an.nrpc += 1
		an.sent_bytes += isz
		an.sent_bytes_breakdown['failure_repair_write'] += isz
	return None

    def add_node (my, t, id):
	# XXX the default dhash add_node tracks
	#     availability but there's no point in doing
	#     that for an oracle.
	newevs = chord.add_node (my, t, id)
	my.repair (t, my.allnodes[id])
	return None

    def min_pieces (my):
        return 1
    def read_pieces (my):
        return 1
    def insert_pieces (my):
        return 1
    def look_ahead (my):
	return 16
    def insert_piece_size (my, size):
        return size

class durability_oracle (dhash_oracle):
    """
    Only has to repair before a node leaves permanently, taking away
    the last copy.
    """
    def fail_node (my, t, id):
	chord.fail_node (my, t, id)
    def crash_node (my, t, id):
	try:
	    n = my.allnodes[id]
	except KeyError:
	    return None
        my._failure (t, id, 1)
	my.repair (t, n)
	return None

class availability_oracle (durability_oracle):
    """
    Has to repair whenever the last copy is about to disappear,
    even temporarily, in addition to what durability_oracle does.
    """
    def fail_node (my, t, id):
	try:
	    n = my.allnodes[id]
	except KeyError:
	    return None
        my._failure (t, id, 0)
	my.repair (t, n)
	return None
