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

