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
unsigned k_bucket_tree::_k = Kademlia::k();


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
  lookup_args la(_id, ip(), _id);
  lookup_result lr;
  KDEBUG(2) << "join: lookup my id.  included ip = " << la.ip << endl;
  doRPC(wkn, &Kademlia::do_lookup, &la, &lr);

  // put well known node in k-buckets
  _tree->insert(lr.rid, wkn);
  dump();

  // insert all results in our k-buckets and find the NodeID closest to our own.
  for(unsigned i=0; i<lr.results.size(); i++) {
    KDEBUG(2) << "join: lookup my id: node " << printID(lr.results[i].first) << endl;
    if(lr.results[i].first != _id)
      _tree->insert(lr.results[i].first, lr.results[i].second);
  }

  // get our ``successor''
  pair<NodeID, IPAddress> *p = get_closest(&(lr.results), _id);

  // common prefix length
  unsigned cpl = common_prefix(_id, p->first);
  KDEBUG(2) << "join: successor is " << printbits(p->first) << ", cpl = " << cpl << endl;

  // all entries further away than him need to be refreshed.
  // see section 2.3
  for(int i=cpl-1; i>=0; i--) {
    // XXX: should be random
    lookup_args la(_id, ip(), (_id ^ (1<<i)));
    lookup_result lr;
    KDEBUG(3) << "join: looking up entry " << i << ": " << printbits(la.key) << " at IP = " << p->second << endl;
    doRPC(p->second, &Kademlia::do_lookup, &la, &lr);

    for(unsigned j=0; j<lr.results.size(); j++) {
      KDEBUG(3) << "join: looking result for entry " << j << ": " << printbits(lr.results[j].first) << endl;
      if(lr.results[j].first != _id)
        _tree->insert(la.key, lr.results[j].second);
    }
  }

  // now get the keys from our successor
  transfer_args ta;
  transfer_result tr;
  ta.id = _id;
  ta.ip = ip();
  KDEBUG(2) << "join: Node " << printbits(_id) << " initiating transfer from " << printbits(p->first) << endl;
  doRPC(p->second, &Kademlia::do_transfer, &ta, &tr);

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

  lookup_args la(_id, ip(), iargs->key);
  lookup_result lr;
  do_lookup(&la, &lr);

  // if we are the successor for this key, we store it.
  pair<NodeID, IPAddress> *p = get_closest(&(lr.results), iargs->key);
  if(p->first == _id) {
    KDEBUG(2) << "Node " << printID(_id) << " storing " << printID(iargs->key) << ":" << iargs->val << "." << endl;
    _values[iargs->key] = iargs->val;
    return;
  }

  // we're not the one to insert it
  doRPC(p->second, &Kademlia::do_insert, iargs, iresult);
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
  KDEBUG(3) << "do_lookup: id = " << printbits(largs->id) << ", ip = " << largs->ip << ", key = " << printbits(largs->key) << endl;
  dump();
  NodeID callerID = largs->id;
  IPAddress callerIP = largs->ip;
  KDEBUG(3) << "do_lookup: callerIP = " << callerIP << endl;

  vector<pair<NodeID, IPAddress> > *bestset = new vector<pair<NodeID, IPAddress> >;
  assert(bestset);

  // deal with the empty case
  if(_tree->empty()) {
    KDEBUG(3) << "do_lookup: tree is empty. returning myself, ip = " << ip() << endl;
    lresult->results.push_back(make_pair(_id, ip()));
    goto done;
  }

  // get the best fitting entry in the tree
  _tree->get(largs->key, bestset);

  // if we don't know how to forward this key or if we are closer
  // are we closer? 
  // XXX: try for all in vector
  KDEBUG(3) << "do_lookup: closest node = " << printbits((*bestset)[0].first) << endl;
  if(!(*bestset)[0].second || distance(_id, largs->key) < distance((*bestset)[0].first, largs->key)) {
    // i am the best
    KDEBUG(3) << "do_lookup: i am the best match" << endl;
    lresult->results.push_back(make_pair(_id, ip()));

  // recursive lookup
  } else {
    KDEBUG(3) << "do_lookup: recursive lookup to " << printbits((*bestset)[0].first) << " at ip = " << (*bestset)[0].second << endl;
    largs->id = _id;
    largs->ip = ip();
    if(!doRPC((*bestset)[0].second, &Kademlia::do_lookup, largs, lresult))
      _tree->erase((*bestset)[0].first);
  }

done:
  // set correct return data
  lresult->rid = _id;

  // insert caller into our tree
  if(callerID != _id)
    _tree->insert(callerID, callerIP);
}

// }}}
// {{{ Kademlia::do_lookup_wrapper

// wrapper around do_lookup(lookup_args *largs, lookup_result *lresult)
// if use_ip == 0, use the well-known node
void
Kademlia::do_lookup_wrapper(IPAddress use_ip, Kademlia::NodeID key, 
    vector<pair<NodeID, IPAddress> > *v)
{
  lookup_args la(_id, ip(), key);
  lookup_result lr;

  doRPC(use_ip ? use_ip : _wkn, &Kademlia::do_lookup, &la, &lr);
  if(!v)
    return;

  // XXX: better copy?
  for(vector<pair<NodeID, IPAddress> >::const_iterator i = lr.results.begin(); i != lr.results.end(); ++i)
    v->push_back(*i);
}

// }}}
// {{{ Kademlia::do_ping
void
Kademlia::do_ping(ping_args *pargs, ping_result *presult)
{
  if(pargs->id != _id)
    _tree->insert(pargs->id, pargs->ip);
}

// }}}
// {{{ Kademlia::do_ping_wrapper
bool
Kademlia::do_ping_wrapper(IPAddress use_ip, ping_args *pargs, ping_result *presult)
{
  assert(use_ip);
  return doRPC(use_ip, &Kademlia::do_ping, pargs, presult);
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
// {{{ Kademlia::common_prefix
unsigned
Kademlia::common_prefix(Kademlia::NodeID k1, Kademlia::NodeID k2)
{
  unsigned size = 0;
  for(unsigned i=0; i<idsize; i++)
    if(getbit(k1, i) == getbit(k2, i))
      size++;
    else
      break;
  return size;
}

// }}}
// {{{ Kademlia::get_closest
pair<Kademlia::NodeID, IPAddress> *
Kademlia::get_closest(vector<pair<NodeID, IPAddress> > *v, NodeID id)
{
  // insert all results in our k-buckets and find the NodeID closest to our own.
  NodeID closestID = ~id;
  pair<NodeID, IPAddress> *closestP = 0;
  for(unsigned i=0; i<v->size(); i++) {
    NodeID xid = (*v)[i].first;
    if(distance(id, xid) < distance(closestID, xid)) {
      closestID = xid;
      closestP = &((*v)[i]);
    }
  }

  return closestP;
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
void
k_bucket_tree::insert(NodeID id, IPAddress ip)
{
  KDEBUG(2) << "k_bucket_tree::insert: BEFORE id = " << Kademlia::printbits(id) << ", ip = " << ip << endl;
  _self->dump();
  peer_t *p = _root->insert(id, ip);
  KDEBUG(2) << "k_bucket_tree::insert: AFTER id = " << Kademlia::printbits(id) << ", ip = " << ip << endl;
  _self->dump();
  _nodes[id] = p;
}

// }}}
// {{{ k_bucket_tree::erase
void
k_bucket_tree::erase(NodeID id)
{
  KDEBUG(2) << "k_bucket_tree::erase: id = " << Kademlia::printbits(id) << endl;
  _nodes.erase(id);
  _self->dump();
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
void
k_bucket_tree::get(NodeID key, vector<pair<Kademlia::NodeID, IPAddress> > *v)
{
  // XXX heinously inefficient, but who cares for now.
  vector<pair<Kademlia::NodeID, IPAddress> > tmp;
  for(map<NodeID, peer_t*>::const_iterator i = _nodes.begin(); i != _nodes.end(); i++)
    tmp.push_back(make_pair(i->first, i->second->ip));

  SortNodes sn(key);
  sort(tmp.begin(), tmp.end(), sn);

  unsigned j = 0;
  for(vector<pair<Kademlia::NodeID, IPAddress> >::const_iterator i = tmp.begin(); i != tmp.end() && j<_k; ++i, ++j)
    v->push_back(*i);
  // _root->get(key, v);
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
peer_t*
k_bucket::insert(Kademlia::NodeID node, IPAddress ip, string prefix, unsigned depth, k_bucket *root)
{
  if(!root)
    root = this; // i.e. Kademlia::_root

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
      _nodes->insert(*it); // so that it gets resorted.  will go to tail.
      KDEBUG(4) <<  "insert: node " << Kademlia::printbits(node) << " already in tree" << endl;
      return *it;
    }

  // if not full, just add the new id.
  // XXX: is this correct? there is this thing that we have to split when our
  // own ID falls in the range of the lowest and highest
  if(_nodes->size() < _k) {
    KDEBUG(4) <<  "insert: added on level " << depth << endl;
    peer_t *p = new peer_t(node, ip, now());
    _nodes->insert(p);
    return p;
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
    return 0;

  // evict the dead one
  _nodes->erase(least_recent);
  _root->erase((*least_recent)->id);

  // insert the new one
  peer_t *p = new peer_t(node, ip, now());
  _nodes->insert(p);

  //
  // Now if this k-bucket contains the node own ID, then split it.
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
      vector<pair<NodeID, IPAddress> > *best = new vector<pair<NodeID, IPAddress> >;
      _root->get((*it)->id, best);
      KDEBUG(1) << "stabilize: lookup for " << Kademlia::printbits((*it)->id) << endl;
      _self->do_lookup_wrapper((*best)[0].second, (*it)->id);
      delete best;
    }
    return;
  }

  // now lookup a random key in this range
  NodeID mask = 0;
  for(unsigned i=0; i<depth; i++)
    mask |= (1<<(Kademlia::idsize-depth-i));

  KDEBUG(1) << "stabilize: mask = " << Kademlia::printbits(mask) << endl;
  NodeID random_key = _self->id() & mask;

  vector<pair<NodeID, IPAddress> > *best = new vector<pair<NodeID, IPAddress> >;
  _root->get(random_key, best);
  for(vector<pair<NodeID, IPAddress> >::const_iterator i = best->begin(); i != best->end(); ++i) {
    KDEBUG(1) << "stabilize: random lookup for " << Kademlia::printbits(random_key) << endl;
    _self->do_lookup_wrapper(i->second, random_key);
  }
  delete best;

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

//
// Return the k closest entries to
//
// void
// k_bucket::get(NodeID key, vector<pair<Kademlia::NodeID, IPAddress> > *v, unsigned depth)
// {
//   // descend into the tree
//   unsigned b = Kademlia::getbit(key, depth);
//   KDEBUG(3) << "do_lookup: bit " << depth << " of key is " << b << endl;
//   if(_child[b]) {
//     KDEBUG(3) << "do_lookup: descending further down from depth " << depth << endl;
//     _child[b]->get(key, v, depth+1);
//     if(v->size() >= _k)
//       return;
//   }
// 
//   KDEBUG(3) << "do_lookup: found deepest level on depth = " << depth << endl;
// 
//   if(!_nodes || !_nodes->size()) {
//     KDEBUG(3) << "do_lookup: returning 0 from depth = " << depth << endl;
//     // XXX
//     return;
//   }
// 
//   // XXX: use alpha
//   NodeID bestdist = (NodeID) -1;
//   for(set<peer_t*>::const_iterator it = _nodes->begin(); it != _nodes->end(); ++it) {
//     NodeID dist = Kademlia::distance(key, (*it)->id);
//     if(dist < bestdist) {
//       bestdist = dist;
//       p = *it;
//     }
//   }
//   assert(p);
//   KDEBUG(3) << "do_lookup: returning best = " << Kademlia::printbits(p->id) << " from depth = " << depth << endl;
//   return p;
// }

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
      cout << "   *** " << prefix << " [" << i++ << "] : " << Kademlia::printbits((*it)->id) << ", firstts = " << (*it)->firstts << ", lastts = " << (*it)->lastts << endl;
  }
}

// }}}

// {{{ trash
#if 0
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
#endif
/// }}}
