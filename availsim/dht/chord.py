import bisect
from node import node

class chord (object):
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
	newevs = None
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
	return None
    
    def _failure (my, t, id, crash):
	try:
	    n = my.allnodes[id]
	except KeyError:
	    return None
        if n.alive:
            if crash:
                n.crash (t)
            else:
                n.stop (t)
            d = bisect.bisect (my.nodes, n)
            assert my.nodes[d - 1] == n, "Expected %s got %s." % (n, my.nodes[d-1])
            my.nodes.pop (d - 1)
            my.deadnodes[n.id] = n
	return None
        
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

