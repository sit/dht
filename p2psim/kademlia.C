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

#define KADEMLIA_REFRESH 100000

unsigned kdebugcounter = 1;
unsigned Kademlia::_k = 1;
Kademlia::NodeID Kademlia::_rightmasks[8*sizeof(Kademlia::NodeID)];

unsigned k_bucket::_k = Kademlia::k();


// XXX: hack for now.
IPAddress kademlia_wkn_ip = 0;
// }}}
// {{{ Kademlia::Kademlia
Kademlia::Kademlia(Node *n)
  : DHTProtocol(n), _id(ConsistentHash::ip2chid(n->ip()) & 0x0000ffff)
{
  KDEBUG(1) << "id: " << printbits(_id) << endl;
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
  if(!kademlia_wkn_ip)
    kademlia_wkn_ip = wkn;

  if(wkn == ip()) {
    KDEBUG(1) << "Node " << printID(_id) << " is wellknown." << endl;
    return;
  }

  // lookup my own key with well known node.
  lookup_args la;
  lookup_result lr;
  la.id = la.key = _id;
  la.ip = ip();
  KDEBUG(2) << "join: lookup my id" << endl;
  doRPC(wkn, &Kademlia::do_lookup, &la, &lr);
  KDEBUG(2) << "join: lookup my id: node " << printID(lr.id) << endl;

  // put well known dude in table
  unsigned entry = _tree->insert(lr.rid, wkn);
  dump();

  // put reply (i.e., ``successor'' of our own ID) in table
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
  NodeID callerID = largs->id;
  NodeID callerIP = largs->ip;

  // deal with the empty case
  if(_tree->empty()) {
    KDEBUG(3) << "do_lookup: tree is empty. returning myself" << endl;
    lresult->id = _id;
    lresult->ip = ip();
    goto done;
  }

  // get the best fitting entry in the tree
  pp = _tree->get(largs->key);

  // are we closer? 
  // XXX: try for all in vector
  KDEBUG(3) << "do_lookup: closest node = " << printbits(pp.first) << endl;
  if(distance(_id, largs->key) < distance(pp.first, largs->key)) {
    // i am the best
    KDEBUG(3) << "do_lookup: i am the best match" << endl;
    lresult->id = _id;
    lresult->ip = ip();
    lresult->rid = _id;

  // recursive lookup
  } else {
    KDEBUG(3) << "do_lookup: recursive lookup to " << printbits(pp.first) << endl;
    largs->id = _id;
    largs->ip = ip();
    doRPC(pp.second, &Kademlia::do_lookup, largs, lresult);
  }

done:
  // set correct return data
  lresult->rid = _id;

  // insert caller into our tree
  _tree->insert(callerID, callerIP);

  dump();
}

// }}}
// {{{ Kademlia::do_lookup_wrapper

// wrapper around do_lookup(lookup_args *largs, lookup_result *lresult)
pair<Kademlia::NodeID, IPAddress>
Kademlia::do_lookup_wrapper(IPAddress use_ip, Kademlia::NodeID key)
{
  lookup_args la;
  lookup_result lr;
  la.id = _id;
  la.ip = ip();
  la.key = key;

  doRPC(use_ip, &Kademlia::do_lookup, &la, &lr);
  return make_pair(lr.id, lr.ip);
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
  stabilize();
  delaycb(STABLE_TIMER, &Kademlia::reschedule_stabilizer, (void *) 0);
}

// }}}
// {{{ Kademlia::do_transfer
void
Kademlia::do_transfer(transfer_args *targs, transfer_result *tresult)
{
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
  cout << "*** DUMP FOR " << printbits(_id) << endl;
  printf("_tree = %p\n", _tree);
  cout << "*** -------------------------- ***" << endl;
  _tree->dump();
  cout << "*** -------------------------- ***" << endl;
}

// }}}

// {{{ k_bucket_tree::k_bucket_tree
k_bucket_tree::k_bucket_tree(Kademlia *k) : _self(k)
{
  _nodes.clear();
  _root = new k_bucket(_self);

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
  pair<peer_t*, unsigned> pp = _root->insert(id, ip);
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
  return make_pair(p->id, p->ip);
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
k_bucket::k_bucket(Kademlia *k) : _self(k)
{
  _child[0] = _child[1] = 0;
  _nodes.clear();

  _id = _self->id(); // for KDEBUG purposes only
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
  for(unsigned i=0; i<_nodes.size(); i++)
    delete _nodes[i];
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
  if(_child[leftmostbit]) {
    return _child[leftmostbit]->insert(node, ip, prefix + (leftmostbit ? "1" : "0"), depth+1, root);
  }


  // is this thing is already in the array, bail out.
  // XXX: move it forward
  for(unsigned i=0; i<_nodes.size(); i++)
    if(_nodes[i]->id == node) {
      _nodes[i]->lastts = now();
      KDEBUG(4) <<  "insert: node " << Kademlia::printbits(node) << " already in tree" << endl;
      return make_pair(_nodes[i], depth);
    }

  // if not full, just add the new id.
  // XXX: is this correct? do we not split on own ID?
  if(_nodes.size() < _k) {
    KDEBUG(4) <<  "insert: added on level " << depth << endl;
    peer_t *p = new peer_t(node, ip, now());
    _nodes.push_back(p);
    return make_pair(p, depth);
  }

  // must be equal then.
  assert(_nodes.size() == _k);
  assert(_child[0] == 0);
  assert(_child[1] == 0);

  _child[0] = new k_bucket(_self);
  _child[1] = new k_bucket(_self);

  // now divide contents into separate buckets
  for(unsigned i=0; i<_nodes.size(); i++) {
    unsigned bit = Kademlia::getbit(_nodes[i]->id, depth);
    _child[bit]->_nodes.push_back(_nodes[i]);
  }
  _nodes.clear();

  // now insert the node
  return root->insert(node, ip, "", 0, root);
}

// }}}
// {{{ k_bucket::stabilize
void
k_bucket::stabilize(string prefix, unsigned depth)
{
  // go through tree depth-first and refresh buckets in the leaves of the tree.
  if(_child[0]) {
    assert(_child[1]);
    _child[0]->stabilize(prefix + "0", depth+1);
    _child[1]->stabilize(prefix + "1", depth+1);
    return;
  }

  for(unsigned i=0; i<_nodes.size(); i++) {
    if(now() - _nodes[i]->lastts < KADEMLIA_REFRESH)
      continue;

    // XXX: where?
    KDEBUG(1) << "stabilize: lookup for " << Kademlia::printbits(_nodes[i]->id) << endl;
    pair<NodeID, IPAddress> pp = _self->do_lookup_wrapper(kademlia_wkn_ip, _nodes[i]->id);
    _nodes[i]->id = pp.first;
    _nodes[i]->ip = pp.second;
  }
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

  if(_nodes.size())
    return true;


  KDEBUG(2) << "stabilized: " << prefix << " not present" << endl;
  NodeID lower_mask = 0;

  //
  // Node claims there is no node to satisfy this entry in the finger table.
  // Check whether that is true.
  //

  // On every iteration we add another bit.  lower_mask looks like 000...111,
  // but we use it as 111...000 by ~-ing it.
  for(unsigned i=0; i<depth; i++)
    lower_mask |= (1<<i);

  // flip the bit, and turn all bits to the right of the flipped bit into
  // zeroes.
  NodeID lower = _id ^ (1<<depth);
  lower &= ~lower_mask;

  // upper bound is the id with one bit flipped and all bits to the right of
  // that turned into ones.
  NodeID upper = lower | lower_mask;

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
  if(_child[b]) {
    KDEBUG(3) << "do_lookup: descending further down" << endl;
    return _child[b]->get(key, depth+1);
  }

  KDEBUG(3) << "do_lookup: found deepest level" << endl;

  assert(_nodes.size());

  // XXX: use alpha
  return _nodes[0];
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

  for(unsigned i=0; i<_nodes.size(); i++)
    if(_nodes[i])
      cout << "*** " << prefix << " [" << i << "] : " << Kademlia::printbits(_nodes[i]->id) << endl;
}

// }}}
