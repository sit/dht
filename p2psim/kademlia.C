// {{{ headers
#include "kademlia.h"
#include "chord.h"
#include "packet.h"
#include "nodefactory.h"
#include <stdio.h>
#include <algorithm>
#include <iostream>
#include "p2psim.h"
using namespace std;

#define KADEMLIA_REFRESH 1000

unsigned kdebugcounter = 1;
unsigned Kademlia::_k = 20;
Kademlia::NodeID Kademlia::_rightmasks[8*sizeof(Kademlia::NodeID)];

unsigned k_bucket::_k = Kademlia::k();


// XXX: hack for now.
IPAddress kademlia_wkn_ip = 0;
// }}}
// {{{ Kademlia::Kademlia
Kademlia::Kademlia(Node *n)
  : DHTProtocol(n), _id(ConsistentHash::ip2chid(n->ip()) & 0x0000ffff), _joined(false)
{
  KDEBUG(1) << "ip: " << ip() << endl;
  _values.clear();

  // precompute masks
  if(!_rightmasks[0]) {
    NodeID mask = 0;
    _rightmasks[0] = (NodeID) -1;
    for(unsigned i=1; i<idsize; i++) {
      mask |= (1<<(i-1));
      _rightmasks[i] = ~mask;
    }
  }
  _tree = new k_bucket_tree(this);
  assert(_tree);
}

// }}}
// {{{ Kademlia::~Kademlia
Kademlia::~Kademlia()
{
}

// }}}
// {{{ Kademlia::join
void
Kademlia::join(Args *args)
{
  IPAddress wkn = args->nget<IPAddress>("wellknown");
  if(!_wkn)
    _wkn = wkn;

  if(wkn == ip()) {
    KDEBUG(1) << "Node " << printID(_id) << " is wellknown." << endl;
    _joined = true;
    return;
  }

  // lookup my own key with well known node.
  lookup_args la;
  lookup_result lr;
  la.id = la.key = _id;
  la.ip = ip();
  KDEBUG(2) << "join: lookup my id.  included ip = " << la.ip << endl;
  doRPC(wkn, &Kademlia::do_lookup, &la, &lr);
  KDEBUG(2) << "join: lookup my id: node " << printID(lr.id) << endl;

  // put well known dude in table
  unsigned entry = _tree->insert(lr.rid, wkn);
  dump();

  // put reply (i.e., ``successor'' of our own ID) in table
  if(lr.id != _id)
    _tree->insert(lr.id, lr.ip);

  // all entries further away than him need to be refereshed.
  // see section 2.3
  IPAddress succIP = lr.ip;
  for(int i=entry-1; i>=0; i--) {
    // XXX: should be random
    la.key = (_id ^ (1<<i));
    KDEBUG(3) << "join: looking up entry " << i << ": " << printbits(la.key) << " at IP = " << succIP << endl;
    doRPC(succIP, &Kademlia::do_lookup, &la, &lr);
    KDEBUG(3) << "join: looking result for entry " << i << ": " << printbits(lr.id) << endl;
    if(lr.id != _id)
      _tree->insert(la.key, lr.ip);
  }

  // now get the keys from our successor
  transfer_args ta;
  transfer_result tr;
  ta.id = _id;
  ta.ip = ip();
  KDEBUG(2) << "join: Node " << printbits(_id) << " initiating transfer from " << printbits(lr.id) << endl;
  doRPC(succIP, &Kademlia::do_transfer, &ta, &tr);

  // merge that data in our _values table
  for(map<NodeID, Value>::const_iterator pos = tr.values.begin(); pos != tr.values.end(); ++pos)
    _values[pos->first] = pos->second;

  delaycb(STABLE_TIMER, &Kademlia::reschedule_stabilizer, (void *) 0);
  _joined = true;
}

// }}}
// {{{ Kademlia::leave
void
Kademlia::leave(Args*)
{
  cout << "Kademlia leave" << endl;
}

// }}}
// {{{ Kademlia::insert
void
Kademlia::insert(Args *args)
{
  insert_args ia;
  insert_result ir;

  ia.id = _id;
  ia.ip = ip();

  ia.key = args->nget<NodeID>("key");
  ia.val = args->nget<Value>("val");

  KDEBUG(2) << "insert " << printID(ia.key) << ":" << ia.val << endl;
  do_insert(&ia, &ir);
}

// }}}
// {{{ Kademlia::do_insert
void
Kademlia::do_insert(insert_args *iargs, insert_result *iresult)
{
  if(iargs->id != _id)
    _tree->insert(iargs->id, iargs->ip);

  lookup_args la;
  lookup_result lr;
  la.id = _id;
  la.ip = ip();
  la.key = iargs->key;
  do_lookup(&la, &lr);

  if(lr.id == _id) {
    KDEBUG(2) << "Node " << printID(_id) << " storing " << printID(iargs->key) << ":" << iargs->val << "." << endl;
    _values[iargs->key] = iargs->val;
    return;
  }

  // we're not the one to insert it
  doRPC(lr.ip, &Kademlia::do_insert, iargs, iresult);
}

// }}}
// {{{ Kademlia::lookup
void
Kademlia::lookup(Args *args)
{
  cout << "Kademlia lookup" << endl;
}

// }}}
// {{{ Kademlia::do_lookup
void
Kademlia::do_lookup(lookup_args *largs, lookup_result *lresult)
{
  pair<NodeID, IPAddress> pp;
  KDEBUG(3) << "do_lookup: id = " << printbits(largs->id) << ", ip = " << largs->ip << ", key = " << printbits(largs->key) << endl;
  dump();
  NodeID callerID = largs->id;
  IPAddress callerIP = largs->ip;
  KDEBUG(3) << "do_lookup: callerIP = " << callerIP << endl;

  // deal with the empty case
  if(_tree->empty()) {
    KDEBUG(3) << "do_lookup: tree is empty. returning myself, ip = " << ip() << endl;
    lresult->id = _id;
    lresult->ip = ip();
    goto done;
  }

  // get the best fitting entry in the tree
  pp = _tree->get(largs->key);

  // if we don't know how to forward this key or if we are closer
  // are we closer? 
  // XXX: try for all in vector
  KDEBUG(3) << "do_lookup: closest node = " << printbits(pp.first) << endl;
  if(!pp.second || distance(_id, largs->key) < distance(pp.first, largs->key)) {
    // i am the best
    KDEBUG(3) << "do_lookup: i am the best match" << endl;
    lresult->id = _id;
    lresult->ip = ip();
    lresult->rid = _id;

  // recursive lookup
  } else {
    KDEBUG(3) << "do_lookup: recursive lookup to " << printbits(pp.first) << " at ip = " << pp.second << endl;
    largs->id = _id;
    largs->ip = ip();
    doRPC(pp.second, &Kademlia::do_lookup, largs, lresult);
  }

done:
  // set correct return data
  lresult->rid = _id;

  // insert caller into our tree
  if(callerID != _id)
    _tree->insert(callerID, callerIP);

  dump();
}

// }}}
// {{{ Kademlia::do_lookup_wrapper

// wrapper around do_lookup(lookup_args *largs, lookup_result *lresult)
// if use_ip == 0, use the well-known node
pair<Kademlia::NodeID, IPAddress>
Kademlia::do_lookup_wrapper(IPAddress use_ip, Kademlia::NodeID key)
{
  lookup_args la;
  lookup_result lr;
  la.id = _id;
  la.ip = ip();
  la.key = key;

  doRPC(use_ip ? use_ip : _wkn, &Kademlia::do_lookup, &la, &lr);
  return make_pair(lr.id, lr.ip);
}

// }}}
// {{{ Kademlia::do_ping
void
Kademlia::do_ping(ping_args *pargs, ping_result *presult)
{
  // should we update our tree as a result of a ping.  I don't think so.
  return;
}

// }}}
// {{{ Kademlia::do_ping_wrapper
bool
Kademlia::do_ping_wrapper(IPAddress use_ip, ping_args *pargs, ping_result *presult)
{
  return doRPC(use_ip ? use_ip : _wkn, &Kademlia::do_ping, pargs, presult);
}

// }}}
// {{{ Kademlia::stabilized
bool
Kademlia::stabilized(vector<NodeID> lid)
{
  return _tree->stabilized(lid);
}

// }}}
// {{{ Kademlia::stabilize
void 
Kademlia::stabilize()
{
  _tree->stabilize();
}

// }}}
// {{{ Kademlia::reschedule_stabilizer
void
Kademlia::reschedule_stabilizer(void *x)
{
  // if stabilize blah.
  KDEBUG(3) << "reschedule_stabilizer" << endl;
  stabilize();
  delaycb(STABLE_TIMER, &Kademlia::reschedule_stabilizer, (void *) 0);
}

// }}}
// {{{ Kademlia::do_transfer
void
Kademlia::do_transfer(transfer_args *targs, transfer_result *tresult)
{
  if(targs->id != _id)
    _tree->insert(targs->id, targs->ip);

  KDEBUG(2) << "handle_transfer to node " << printbits(targs->id) << "\n";
  if(_values.size() == 0) {
    KDEBUG(2) << "handle_transfer_cb; no values: done!\n";
    return;
  }

  // XXX: this is wrong, I think.  shouldn't we be using the correct distance
  // metric here?
  //
  // XXX: this is scary because we're deleting nodes before the other guy has
  // them.  what if the reply fails?
  for(map<NodeID, Value>::const_iterator pos = _values.begin(); pos != _values.end(); ++pos)
    if(pos->first >= targs->id) {
      tresult->values.insert(*pos);
      _values.erase(pos->first);
    }
}

// }}}
// {{{ Kademlia::getbit
//
// Returns the i-th bit in n.  0 is the most significant bit.
//
inline
unsigned
Kademlia::getbit(NodeID n, unsigned i)
{
  return (n & (1<<((sizeof(NodeID)*8)-i-1))) ? 1 : 0;
}

// }}}
// {{{ Kademlia::flipbitandmaskright
//
// flips the i-th bit and zeroes out the ones right of it.
//
// for example:
//
//   n = 00110110, i = 3
//                     |
//                     v
//       flip bit: 00111110
//           mask: 11111000
//         result: 00111000
//
inline
Kademlia::NodeID
Kademlia::flipbitandmaskright(NodeID n, unsigned i)
{
  assert((i >= 0) && (i < (8*sizeof(NodeID))));
  return ((n ^ (1<<i)) & _rightmasks[i]);
}

// }}}
// {{{ Kademlia::maskright
inline
Kademlia::NodeID
Kademlia::maskright(NodeID n, unsigned i)
{
  return n & _rightmasks[i];
}

// }}}
// {{{ Kademlia::printbits
string
Kademlia::printbits(NodeID id)
{
  char buf[128];

  unsigned j=0;
  for(int i=idsize-1; i>=0; i--)
    sprintf(&(buf[j++]), "%u", (id >> i) & 0x1);
  // sprintf(&(buf[j]), ":%llx", id);
  sprintf(&(buf[j]), ":%hx", id);

  return string(buf);
}

// }}}
// {{{ Kademlia::printID
string
Kademlia::printID(NodeID id)
{
  char buf[128];
  sprintf(buf, "%x", id);
  return string(buf);
}

// }}}
// {{{ Kademlia::distance
Kademlia::NodeID
Kademlia::distance(Kademlia::NodeID from, Kademlia::NodeID to)
{
  return from ^ to;
}

// }}}
// {{{ Kademlia::crash
void
Kademlia::crash(Args*)
{
  cout << "Kademlia crash" << endl;
}

// }}}
// {{{ Kademlia::dump
void
Kademlia::dump()
{
  if(!verbose)
    return;

  cout << "*** DUMP FOR " << printbits(_id) << " (" << _joined << ")" << endl;
  cout << "   *** -------------------------- ***" << endl;
  _tree->dump();
  cout << "   *** -------------------------- ***" << endl;
}

// }}}

// {{{ k_bucket_tree::k_bucket_tree
k_bucket_tree::k_bucket_tree(Kademlia *k) : _self(k)
{
  _nodes.clear();
  _root = new k_bucket(_self, this);

  _id = _self->id(); // for KDEBUG purposes only
}

// }}}
// {{{ k_bucket_tree::~k_bucket_tree
k_bucket_tree::~k_bucket_tree()
{
  delete _root;
}

// }}}
// {{{ k_bucket_tree::insert
unsigned
k_bucket_tree::insert(NodeID id, IPAddress ip)
{
  KDEBUG(2) << "k_bucket_tree::insert: BEFORE id = " << Kademlia::printbits(id) << ", ip = " << ip << endl;
  _self->dump();
  pair<peer_t*, unsigned> pp = _root->insert(id, ip);
  KDEBUG(2) << "k_bucket_tree::insert: AFTER id = " << Kademlia::printbits(id) << ", ip = " << ip << endl;
  _self->dump();
  _nodes.push_back(pp.first);
  return pp.second;
}

// }}}
// {{{ k_bucket_tree::stabilize
void 
k_bucket_tree::stabilize()
{
  _root->stabilize();
}

// }}}
// {{{ k_bucket_tree::stabilized
bool
k_bucket_tree::stabilized(vector<NodeID> lid)
{
  return _root->stabilized(lid);
}

// }}}
// {{{ k_bucket_tree::get
pair<k_bucket_tree::NodeID, IPAddress>
k_bucket_tree::get(NodeID key)
{
  peer_t *p = _root->get(key);
  if(p)
    return make_pair(p->id, p->ip);
  else
    return make_pair(0, 0);

}

// }}}
// {{{ k_bucket_tree::random_node
pair<k_bucket_tree::NodeID, IPAddress>
k_bucket_tree::random_node()
{
  unsigned r = 1 + (unsigned)(((float) _nodes.size())*rand() / (RAND_MAX+1.0));
  return make_pair(_nodes[r]->id, _nodes[r]->ip);
}

// }}}

// {{{ k_bucket::k_bucket
k_bucket::k_bucket(Kademlia *k, k_bucket_tree *root) : _leaf(false), _self(k), _root(root)
{
  _child[0] = _child[1] = 0;
  _id = _self->id(); // for KDEBUG purposes only
  _nodes = new set<peer_t*, SortedByLastTime>;
  _nodes->clear();
  assert(_nodes);
}

// }}}
// {{{ k_bucket::~k_bucket
// depth-first delete
k_bucket::~k_bucket()
{
  if(_child[0])
    delete _child[0];
  if(_child[1])
    delete _child[1];

  if(_nodes) {
    for(set<peer_t*>::const_iterator it = _nodes->begin(); it != _nodes->end(); ++it)
      delete *it;
    delete _nodes;
  }
}

// }}}
// {{{ k_bucket::insert
pair<peer_t*, unsigned>
k_bucket::insert(Kademlia::NodeID node, IPAddress ip, string prefix, unsigned depth, k_bucket *root)
{
  if(!root)
    root = this; // i.e. Kademlia::_root

  // descend to right level in tree
  // if[child[0]], go left
  // if[child[1]], go right
  if(depth == 0)
    KDEBUG(4) << "insert: node = " << Kademlia::printbits(node) << ", ip = " << ip << ", prefix = " << prefix << endl;

  unsigned leftmostbit = Kademlia::getbit(node, depth);
  unsigned myleftmostbit = Kademlia::getbit(_self->id(), depth);
  KDEBUG(4) << "insert: leftmostbit = " << leftmostbit << ", depth = " << depth << endl;

  //
  // NON-ENDLEAF NODE WITH CHILD
  //
  if(_child[leftmostbit]) {
    assert(!_leaf);
    KDEBUG(4) << "insert: _child[" << leftmostbit << "] exists, descending" << endl;
    return _child[leftmostbit]->insert(node, ip, prefix + (leftmostbit ? "1" : "0"), depth+1, root);
  }


  //
  // TRY TO INSERT AT THE NODE WE END UP AT THROUGH RECURSION
  //

  // is this thing is already in the array, bail out.
  // XXX: move it forward
  for(set<peer_t*>::const_iterator it = _nodes->begin(); it != _nodes->end(); ++it)
    if((*it)->id == node) {
      _nodes->erase(*it);
      (*it)->lastts = now();
      _nodes->insert(*it); // so that it gets resorted
      KDEBUG(4) <<  "insert: node " << Kademlia::printbits(node) << " already in tree" << endl;
      return make_pair(*it, depth);
    }

  // if not full, just add the new id.
  // XXX: is this correct? there is this thing that we have to split when our
  // own ID falls in the range of the lowest and highest
  if(_nodes->size() < _k) {
    KDEBUG(4) <<  "insert: added on level " << depth << endl;
    peer_t *p = new peer_t(node, ip, now());
    _nodes->insert(p);
    return make_pair(p, depth);
  }

  //
  // _nodes ARRAY IS FULL.  PING THE LEAST-RECENTLY SEEN NODE.
  //

  assert(_nodes->size() == _k);
  assert(_child[0] == 0);
  assert(_child[1] == 0);

  // ping the least-recently seen node.
  set<peer_t*>::const_iterator least_recent = _nodes->begin();
  if(_self->do_ping_wrapper((*least_recent)->ip, 0, 0))
    return make_pair((peer_t*)0, depth);

  // evict the dead one
  _nodes->erase(least_recent);

  // insert the new one
  peer_t *p = new peer_t(node, ip, now());
  _nodes->insert(p);

  //
  //  Now if this k-bucket contains the node own ID, then split it.
  //
  // XXX: should return make_pair(p, depth);
  if(node >= (*_nodes->begin())->id &&
     node <= (*_nodes->end())->id &&
     (*_nodes->begin())->id != (*_nodes->end())->id)
  {
    assert(!_leaf);

  // split if this is not a final leaf
  // if(!_leaf) {
    // create both children
    KDEBUG(4) <<  "insert: not a leaf.  creating subchildren" << endl;
    _child[0] = new k_bucket(_self, _root);
    _child[1] = new k_bucket(_self, _root);
    _child[myleftmostbit ^ 1]->_leaf = true;
    KDEBUG(4) <<  "insert: subchild " << (myleftmostbit ^ 1) << " is a leaf on depth " << depth << endl;

    // now divide contents into separate buckets
    // XXX: we have to ping these guys?
    for(set<peer_t*>::const_iterator it = _nodes->begin(); it != _nodes->end(); ++it) {
      unsigned bit = Kademlia::getbit((*it)->id, depth);
      KDEBUG(4) <<  "insert: pushed entry " << Kademlia::printbits((*it)->id) << " to side " << bit << endl;
      _child[bit]->_nodes->insert(*it);
    }
    delete _nodes;
    _nodes = 0;

    // now insert at the right child
    KDEBUG(4) <<  "insert: after split, calling insert for prefix " << (prefix + (leftmostbit ? "1" : "0")) << " to depth " << (depth+1) << endl;
    return _child[leftmostbit]->insert(node, ip, prefix + (leftmostbit ? "1" : "0"), depth+1, root);
  }

  // this is a leaf and it's full.
  // XXX: evict one. oh wait, we're leaving the more trustworthy ones in.
  // KDEBUG(4) <<  "insert: last make_pair on depth " << depth << endl;
  // return make_pair((peer_t*)0, depth);
}

// }}}
// {{{ k_bucket::stabilize
void
k_bucket::stabilize(string prefix, unsigned depth)
{
  // go through tree depth-first and refresh buckets in the leaves of the tree.
  if(_child[0]) {
    assert(!_nodes);
    assert(_child[1]);
    _child[0]->stabilize(prefix + "0", depth+1);
    _child[1]->stabilize(prefix + "1", depth+1);
    return;
  }

  assert(_nodes);
  if(_nodes->size()) {
    // make a temporary copy
    set<peer_t*, SortedByLastTime> tmpcopy(*_nodes);
    for(set<peer_t*>::const_iterator it = tmpcopy.begin(); it != tmpcopy.end(); ++it) {
      if(now() - (*it)->lastts < KADEMLIA_REFRESH)
        continue;

      // find the closest node to the ID we're looking for
      pair<NodeID, IPAddress> pp = _root->get((*it)->id);
      KDEBUG(1) << "stabilize: lookup for " << Kademlia::printbits((*it)->id) << endl;
      _self->do_lookup_wrapper(pp.second, (*it)->id);
    }
    return;
  }

  // now lookup a random key in this range
  NodeID mask = 0;
  for(unsigned i=0; i<depth; i++)
    mask |= (1<<(Kademlia::idsize-depth-i));

  KDEBUG(1) << "stabilize: mask = " << Kademlia::printbits(mask) << endl;
  NodeID random_key = _self->id() & mask;
  pair<NodeID, IPAddress> pp = _root->get(random_key);
  KDEBUG(1) << "stabilize: random lookup for " << Kademlia::printbits(random_key) << endl;
  _self->do_lookup_wrapper(pp.second, random_key);

  // NB: the lookup itself will add it to the tree!
}

// }}}
// {{{ k_bucket::stabilized
bool
k_bucket::stabilized(vector<NodeID> lid, string prefix, unsigned depth)
{
  // 
  // example node ID 01001011
  //
  // look at second entry in finger table should yield a node ID
  // less than 01001001, but larger than 01001000
  // (we preserved prefix)
  //
  // for fourth entry in finger table should
  // less than 01000111, larger than 01000000
  //
  if(_child[0]) {
    assert(_child[1]);
    return _child[0]->stabilized(lid, prefix + "0", depth+1) &&
           _child[1]->stabilized(lid, prefix + "1", depth+1);
  }

  if(!_nodes || _nodes->size())
    return true;


  KDEBUG(2) << "stabilized: " << prefix << " not present, depth = " << depth << ", prefix = " << prefix << endl;
  NodeID lower_mask = 0;

  //
  // Node claims there is no node to satisfy this entry in the finger table.
  // Check whether that is true.
  //

  // On every iteration we add another bit.  lower_mask looks like 111...000,
  // but we use it as 000...111 by ~-ing it.
  for(unsigned i=0; i<depth; i++)
    lower_mask |= (1<<(Kademlia::idsize-i-1));
  KDEBUG(4) << "stabilized: lower_mask on depth " << depth << " = " << Kademlia::printbits(lower_mask) << endl;

  // flip the bit, and turn all bits to the right of the flipped bit into
  // zeroes.
  NodeID lower = _id ^ (1<<(Kademlia::idsize-depth-1));
  KDEBUG(4) << "stabilized: lower before mask = " << Kademlia::printbits(lower) << endl;
  lower &= lower_mask;
  KDEBUG(4) << "stabilized: lower after mask = " << Kademlia::printbits(lower) << endl;

  // upper bound is the id with one bit flipped and all bits to the right of
  // that turned into ones.
  NodeID upper = lower | ~lower_mask;

  KDEBUG(4) << "stabilized: lower = " << Kademlia::printbits(lower) << endl;
  KDEBUG(4) << "stabilized: upper = " << Kademlia::printbits(upper) << endl;

  // yields the node with smallest id greater than lower
  vector<NodeID>::const_iterator it = upper_bound(lid.begin(), lid.end(), lower);

  // check that this is smaller than upper.  if so, then this node would
  // qualify for this entry in the finger table, so the node that says there
  // is no such is WRONG.
  if(it != lid.end() && *it <= upper) {
    KDEBUG(4) << "stabilized: entry " << depth << " is invalid, but " << Kademlia::printbits(*it) << " matches " << endl;
    KDEBUG(4) << "stabilized: lowermask = " << Kademlia::printbits(lower_mask) << endl;
    KDEBUG(4) << "stabilized: ~lowermask = " << Kademlia::printbits(~lower_mask) << endl;
    KDEBUG(4) << "stabilized: lower = " << Kademlia::printbits(lower) << endl;
    KDEBUG(4) << "stabilized: upper = " << Kademlia::printbits(upper) << endl;
    return false;
  }

  return true;
}

// }}}
// {{{ k_bucket::get
peer_t*
k_bucket::get(NodeID key, unsigned depth)
{
  // descend into the tree
  unsigned b = Kademlia::getbit(key, depth);
  KDEBUG(3) << "do_lookup: bit " << depth << " of key is " << b << endl;
  peer_t *p = 0;
  if(_child[b]) {
    KDEBUG(3) << "do_lookup: descending further down" << endl;
    if((p = _child[b]->get(key, depth+1)))
      return p;
  }

  KDEBUG(3) << "do_lookup: found deepest level on depth = " << depth << endl;

  if(!_nodes || !_nodes->size()) {
    KDEBUG(3) << "do_lookup: returning 0 from depth = " << depth << endl;
    return 0;
  }

  // XXX: use alpha
  NodeID bestdist = (NodeID) -1;
  for(set<peer_t*>::const_iterator it = _nodes->begin(); it != _nodes->end(); ++it) {
    NodeID dist = Kademlia::distance(key, (*it)->id);
    if(dist < bestdist) {
      bestdist = dist;
      p = *it;
    }
  }
  assert(p);
  KDEBUG(3) << "do_lookup: returning best = " << Kademlia::printbits(p->id) << " from depth = " << depth << endl;
  return p;
}

// }}}
// {{{ k_bucket::dump

void
k_bucket::dump(string prefix, unsigned depth)
{
  if(_child[0]) {
    assert(_child[1]);
    _child[0]->dump(prefix + "0", depth+1);
    _child[1]->dump(prefix + "1", depth+1);
    return;
  }

  unsigned i = 0;
  for(set<peer_t*>::const_iterator it = _nodes->begin(); it != _nodes->end(); ++it) {
    if(*it)
      cout << "   *** " << prefix << " [" << i++ << "] : " << Kademlia::printbits((*it)->id) << ", ts = " << (*it)->lastts << endl;
  }
}

// }}}
