#include "kademlia.h"
#include "packet.h"
#include "nodefactory.h"
#include "chord.h"
#include <stdio.h>
#include <algorithm>
#include <iostream>
#include "p2psim.h"
using namespace std;


unsigned kdebugcounter = 1;
Kademlia::NodeID Kademlia::_rightmasks[8*sizeof(Kademlia::NodeID)];
unsigned Kademlia::_k = 8;

Kademlia::Kademlia(Node *n) : Protocol(n), _id(ConsistentHash::ip2chid(n->ip()) & 0x0000ffff)
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

  _root = new k_bucket();
  assert(_root);
}

Kademlia::~Kademlia()
{
}


bool
Kademlia::stabilized(vector<NodeID> lid)
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
  //

  // NodeID lower_mask = 0;
  // for (unsigned i=0; i<idsize; i++) {
  //   // we're fine with filled entries
  //   // XXX: we could still check this
  //   if(_fingers.valid(i))
  //     continue;

  //   //
  //   // Node claims there is no node to satisfy this entry in the finger table.
  //   // Check whether that is true.
  //   //

  //   // On every iteration we add another bit.  lower_mask looks like 000...111,
  //   // but we use it as 111...000 by ~-ing it.
  //   if(i)
  //     lower_mask |= (1<<(i-1));

  //   // flip the bit, and turn all bits to the right of the flipped bit into
  //   // zeroes.
  //   NodeID lower = _id ^ (1<<i);
  //   lower &= ~lower_mask;

  //   // upper bound is the id with one bit flipped and all bits to the right of
  //   // that turned into ones.
  //   NodeID upper = lower | lower_mask;

  //   // yields the node with smallest id greater than lower
  //   vector<NodeID>::const_iterator it = upper_bound(lid.begin(), lid.end(), lower);

  //   // check that this is smaller than upper.  if so, then this node would
  //   // qualify for this entry in the finger table, so the node that says there
  //   // is no such is WRONG.
  //   if(it != lid.end() && *it <= upper) {
  //     KDEBUG(4) << "stabilized: entry " << i << " is invalid, but " << printbits(*it) << " matches " << endl;
  //     KDEBUG(4) << "stabilized: lowermask = " << printbits(lower_mask) << endl;
  //     KDEBUG(4) << "stabilized: ~lowermask = " << printbits(~lower_mask) << endl;
  //     KDEBUG(4) << "stabilized: lower = " << printbits(lower) << endl;
  //     KDEBUG(4) << "stabilized: upper = " << printbits(upper) << endl;
  //     return false;
  //   }
  // }

  return true;
}


void
Kademlia::join(Args *args)
{
  IPAddress wkn = args->nget<IPAddress>("wellknown");
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
  unsigned entry = _root->insert(lr.rid, wkn);

  // put reply (i.e., ``successor'' of our own ID) in table
  _root->insert(lr.id, lr.ip);

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
      _root->insert(la.key, lr.ip);
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


void
Kademlia::reschedule_stabilizer(void *x)
{
  // if stabilize blah.
  stabilize();
  delaycb(STABLE_TIMER, &Kademlia::reschedule_stabilizer, (void *) 0);
}


void 
Kademlia::stabilize(void)
{
  // go through table and look up all keys
  lookup_args la;
  lookup_result lr;
  la.id = _id;
  la.ip = ip();

  for(unsigned i=0; i<idsize; i++) {
    la.key = (_id ^ (1<<i));
    KDEBUG(3) << "stabilize: looking up entry " << i << ": " << printbits(la.key) << endl;
    do_lookup(&la, &lr);
    KDEBUG(3) << "stabilize: looking result for entry " << i << ": " << printbits(lr.id) << endl;
    if(lr.id != _id)
      _root->insert(la.key, lr.ip);
  }
}


void
Kademlia::do_join(void *args, void *result)
{
  join_args *jargs = (join_args*) args;
  KDEBUG(2) << "do_join " << printbits(jargs->id) << " entering\n";
  _root->insert(jargs->id, jargs->ip);
}


void
Kademlia::do_transfer(void *args, void *result)
{
  transfer_args *targs = (transfer_args*) args;
  transfer_result *tresult = (transfer_result*) result;
  _root->insert(targs->id, targs->ip);

  KDEBUG(2) << "handle_transfer to node " << printID(targs->id) << "\n";
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


//
// Returns the i-th bit in n.  0 is the most significant bit.
//
inline
unsigned
Kademlia::getbit(NodeID n, unsigned i)
{
  return (n & (1<<((sizeof(NodeID)*8)-i-1))) ? 1 : 0;
}



void
Kademlia::do_lookup(void *args, void *result)
{
  lookup_args *largs = (lookup_args*) args;
  lookup_result *lresult = (lookup_result*) result;
  peer_t *closenode = 0;
  k_bucket *kb = _root;

  KDEBUG(3) << "do_lookup: id = " << printbits(largs->id) << ", ip = " << largs->ip << ", key = " << printbits(largs->key) << endl;
  NodeID callerID = largs->id;
  NodeID callerIP = largs->ip;

  // deal with the empty case
  if(!_root->_child[0] && !_root->_child[1] && !_root->_nodes.size()) {
    lresult->id = _id;
    lresult->ip = ip();
    goto done;
  }

  // descend into the tree
  for(unsigned i=0; i<idsize; i++) {
    unsigned b = getbit(largs->key, i);
    KDEBUG(3) << "do_lookup: bit " << i << " of key is " << b << endl;
    if(kb->_child[b]) {
      KDEBUG(3) << "do_lookup: descending further down" << endl;
      kb = kb->_child[b];
      continue;
    }
    KDEBUG(3) << "do_lookup: found deepest level" << endl;
    break;
  }

  // XXX: use alpha
  assert(kb->_nodes.size());

  // are we closer? 
  // XXX: try for all in vector
  closenode = kb->_nodes[0];
  KDEBUG(3) << "do_lookup: closenode = " << printbits(closenode->id) << endl;
  if(distance(_id, largs->key) < distance(closenode->id, largs->key)) {
    // i am the best
    KDEBUG(3) << "do_lookup: i am the best match" << endl;
    lresult->id = _id;
    lresult->ip = ip();
    lresult->rid = _id;

  // recursive lookup
  } else {
    KDEBUG(3) << "do_lookup: recursive lookup to " << printbits(kb->_nodes[0]->id) << endl;
    largs->id = _id;
    largs->ip = ip();
    doRPC(kb->_nodes[0]->ip, &Kademlia::do_lookup, args, result);
  }

done:
  // set correct return data
  lresult->rid = _id;

  // insert caller into our tree
  _root->insert(callerID, callerIP);

}




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

inline
Kademlia::NodeID
Kademlia::maskright(NodeID n, unsigned i)
{
  return n & _rightmasks[i];
}


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


string
Kademlia::printID(NodeID id)
{
  char buf[128];
  sprintf(buf, "%x", id);
  return string(buf);
}



Kademlia::NodeID
Kademlia::distance(Kademlia::NodeID from, Kademlia::NodeID to)
{
  return from ^ to;
}



void
Kademlia::leave(Args*)
{
  cout << "Kademlia leave" << endl;
}

void
Kademlia::crash(Args*)
{
  cout << "Kademlia crash" << endl;
}

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


void
Kademlia::do_insert(void *args, void *result)
{
  insert_args *iargs = (insert_args*) args;
  _root->insert(iargs->id, iargs->ip);

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

  doRPC(lr.ip, &Kademlia::do_insert, &la, &lr);
}


void
Kademlia::lookup(Args *args)
{
  cout << "Kademlia lookup" << endl;
}


/*
 *
 * K-BUCKET
 *
 */


Kademlia::k_bucket *Kademlia::k_bucket::_root = 0;
unsigned Kademlia::k_bucket::_k = Kademlia::k();

Kademlia::k_bucket::k_bucket()
{
  _child[0] = _child[1] = 0;
  if(_root == 0)
    _root = this;
}


// depth-first delete
Kademlia::k_bucket::~k_bucket()
{
  if(_child[0])
    delete _child[0];
  if(_child[1])
    delete _child[1];
  for(unsigned i=0; i<_nodes.size(); i++)
    delete _nodes[i];
}

unsigned
Kademlia::k_bucket::insert(NodeID node, IPAddress ip, NodeID prefix, unsigned depth)
{
  // descend to right level in tree
  // if[child[0]], go left
  // if[child[1]], go right
  DEBUG(4) <<  "insert: node = " << Kademlia::printbits(node) << ", ip = " << ip << ", prefix = " << Kademlia::printbits(prefix) << ", depth = " << depth << endl;
  unsigned leftmostbit = (node & (1<<((sizeof(NodeID)*8)-1))) ? 1 : 0;
  DEBUG(4) <<  "insert: leftmostbit = " << leftmostbit << endl;
  if(_child[leftmostbit]) {
    prefix = (prefix << 1) | (NodeID) leftmostbit;
    DEBUG(4) <<  "insert: descending deeper, going into " << leftmostbit << endl;
    return _child[leftmostbit]->insert(node, ip, prefix, depth+1);
  }

  // if not full, just add the new id.
  // XXX: is this correct? do we not split on own ID?
  if(_nodes.size() < _k) {
    _nodes.push_back(new peer_t(node, ip));
    return depth;
  }

  // must be equal then.
  assert(_nodes.size() == _k);
  assert(_child[0] == 0);
  assert(_child[1] == 0);

  _child[0] = new k_bucket();
  _child[1] = new k_bucket();

  // now divide contents into separate buckets
  for(unsigned i=0; i<_nodes.size(); i++) {
    char bit = node & (1 << ((sizeof(NodeID)*8)-depth-1)) ? 1 : 0;
    prefix = (prefix << 1) | (NodeID) bit;
    _child[bit]->_nodes.push_back(new peer_t(_nodes[i]->id, _nodes[i]->ip));
    _nodes[i] = 0;
  }

  // now insert the node
  return _root->insert(node, ip);
}
