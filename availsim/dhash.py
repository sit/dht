from sha import sha
import bisect
from utils import size_rounder

def between (i, a, b):
    if a < b:
        return a < i and i < b
    if a == b:
        return a == i
    if a > b:
        return i > a or i < b
    
class node:
    """A subsidiary class for DHash nodes"""
    def __init__ (my, id):
        my.id = id
        my.blocks = []
        my.alive = 1
        my.bytes = 0

    def start (my):
        my.alive = 1
    def stop (my):
        my.alive = 0
    
    def crash (my):
        my.stop ()
        my.blocks = []
        my.bytes  = 0

    def store (my, block, size):
        assert my.alive
        if block not in my.blocks:
            bisect.insort (my.blocks, block)
            my.bytes += size
        
    def __cmp__ (my, other):
        return cmp (my.id, other.id)
    def __str__ (my):
        return hex(long(my.id)).lower()[2:-1]

class dhash:
    """A Global View DHash simulator.
    The basic DHash here just stores 3 copies of blocks on successors, no repair.
    """
    def __init__ (my):
        my.nodes = []
        my.deadnodes = {}
        my.allnodes = {}

    def add_node (my, id):
        if id in my.allnodes:
            nnode = my.allnodes[id]
        else:
            nnode = node (id)
            my.allnodes[id] = nnode

        if not nnode.alive:
            # This might be a re-join of some sort
            nnode.start ()
            del my.deadnodes[id]
            
        # Maintain sorted order for nodes.            
        if nnode not in my.nodes:
            bisect.insort (my.nodes, nnode)
        else:
            raise KeyError, "Duplicate insert of %d" % id
    
    def _failure (my, id, crash):
        n = my.allnodes[id]
        if n.alive:
            if crash:
                n.crash ()
            else:
                n.stop ()
            d = bisect.bisect (my.nodes, n)
            assert my.nodes[d - 1] == n
            my.nodes.pop (d - 1)
            my.deadnodes[n.id] = n
            my.repair (n)
        
    def fail_node (my, id):
        my._failure (id, 0)

    def crash_node (my, id):
        my._failure (id, 1)

    def find_successor_index (my, id):
        # Find node that is the successor...
        n = bisect.bisect_left (map(lambda x: x.id, my.nodes), id)
        if n >= len(my.nodes):
            n = 0            
        return n
    def find_predecessor_index (my, id):
        n = bisect.bisect_left (map(lambda x: x.id, my.nodes), id)
        if n == 0:
            n = len (my.nodes)
        n -= 1
        return n

    def succ (my, o, num = 1):
        id = o
        if isinstance (o, node): id = o.id
        n = my.find_successor_index (id)
        diff = n + num - len (my.nodes)
        if diff > 0:
            if diff > n: diff = n
            return my.nodes[n:] + my.nodes[:diff]
        return my.nodes[n:n+num]

    def pred (my, o):
        id = o
        if isinstance (o, node): id = o.id
        n = my.find_predecessor_index (id)
        return my.nodes[n]

    # Subclass and redefine these methods to actually store data.
    def insert_block (my, block, size):
        pass
    def repair (my, failed_node):
        pass
    def availability_summary (my):
        return "Nothing available."

class dhash_replica_norepair (dhash):
    def __init__ (my, replicas = 3):
        dhash.__init__ (my)
        my.bytes = 0
        my.blocks = {}
        my.replicas = replicas
        
    def insert_block (my, block, size):
        if block not in my.blocks:
            my.blocks[block] = size
        succs = my.succ (block, my.replicas)
        for s in succs:
            s.store (block, size)
        return succs[0]

    def repair (my, failed_node):
        pass

    def availability_summary (my):
        blocks = {}
        disk_bytes_used = 0
        # XXX should really search in successors of block based on read
        for n in my.nodes:
            disk_bytes_used += n.bytes
            for b in n.blocks:
                blocks[b] = blocks.get (b, 0) + 1
        avail = 0
        for (b, c) in blocks.items ():
            if c >= 1:
                avail += 1
        return "%d/%d blocks; %s bytes used" % (avail,
                                                len(my.blocks),
                                                size_rounder(disk_bytes_used))

class dhash_replica_lazy (dhash_replica_norepair):
    # XXX How long to wait until we do repair?
    def repair (my, failed_node):
        pred = my.pred (failed_node)
        succs = my.succ (failed_node, my.replicas)
        # Find blocks that we were responsible for
        resp_blocks = filter (lambda b: between (b, pred.id, failed_node.id),
                              my.blocks.keys ())
        for b in resp_blocks:
            # Check their availabiltiy
            avail = 0
            for s in succs:
                if b in s.blocks: avail += 1
            # Lazy repair waits until last possible moment.
            if avail == 1:
                print "REPAIR block", b, "after failure of", failed_node
                for s in succs:
                    if b not in s.blocks:
                        s.store (b, my.blocks[b])

class dhash_fragments (dhash):
    def __init__ (my, dfrags, efrags):
        dhash.__init__ (my)
        my.dfrags = dfrags
        my.efrags = efrags
        my.blocks = {}

    def frag_size (my, size):
        return int (1.02 * (size / my.dfrags))
        
    def insert_block (my, block, size):
        if block not in my.blocks:
            my.blocks[block] = size
        # A vague estimate of overhead from encoding... 2%-ish
        fragsize = my.frag_size (size)
        succs = my.succ (block, my.efrags)
        for s in succs:
            s.store (block, fragsize)
        return succs[0]

    def repair (my, failed_node):
        pred = my.pred (failed_node)
        succs = my.succ (failed_node, my.efrags)
        resp_blocks = filter (lambda b: between (b, pred.id, failed_node.id),
                              my.blocks.keys ())
        for b in resp_blocks:
            fragsize = my.frag_size (my.blocks[b])
            # Check their availabiltiy
            avail = 0
            for s in succs:
                if b in s.blocks: avail += 1
            if avail <= my.efrags: ### efrags? or dfrags?
                print "REPAIR frags for", b, "after failure of", failed_node
                for s in succs:
                    if b not in s.blocks:
                        s.store (b, fragsize)
                        
    def availability_summary (my):
        blocks = {}
        disk_bytes_used = 0
        # XXX should really search in successors of block based on read
        for n in my.nodes:
            disk_bytes_used += n.bytes
            for b in n.blocks:
                blocks[b] = blocks.get (b, 0) + 1
        avail = 0
        for (b, c) in blocks.items ():
            if c >= my.dfrags:
                avail += 1
        return "%d/%d blocks; %s bytes used" % (avail,
                                                len(my.blocks),
                                                size_rounder(disk_bytes_used))


if __name__ == '__main__':
    gdh = dhash_replica_norepair()
    gdh.add_node (55)
    gdh.add_node (4)
    gdh.add_node (23)
    gdh.add_node (30)
    gdh.add_node (17)
    gdh.add_node (42)
    gdh.add_node (63)
    try:
        gdh.add_node (4)
        assert 0, "Should not allow duplicate insert!"
    except KeyError:
        pass

    # Make sure list is sorted
    start = 0
    for n in gdh.nodes:
        assert n.id > start
        start = n.id

    # Do blocks go to the right place?
    assert gdh.insert_block (73, 8192).id == 4
    assert gdh.insert_block (3, 8192).id == 4
    assert gdh.insert_block (4, 8192).id == 4
    assert gdh.insert_block (20, 8192).id == 23
    assert gdh.insert_block (56, 8192).id == 63

    assert gdh.find_predecessor_index (4) == 6
    assert gdh.find_predecessor_index (66) == 6
    assert gdh.find_predecessor_index (10) == 0
    assert gdh.find_predecessor_index (25) == 2

    assert len(gdh.succ(0, 3)) == 3
    assert len(gdh.succ(63, 3)) == 3
    assert len(gdh.succ(24, 3)) == 3
    assert len(gdh.succ(35, 3)) == 3
    
