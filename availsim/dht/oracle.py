from dhash import dhash

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
	return None

    def add_node (my, t, id):
	newevs = dhash.add_node (my, t, id)
	assert (len (newevs) == 0)
	my.repair (t, my.allnodes[id])
	return None

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
	    return None
	my.repair (t, n)
        my._failure (t, id, 1)
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
	my.repair (t, n)
        my._failure (t, id, 0)
	return None

