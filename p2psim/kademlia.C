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

Kademlia::Kademlia(Node *n) : Protocol(n), _id(ConsistentHash::ip2chid(n->ip()) & 0x000f), _fingers(_id)
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

  NodeID lower_mask = 0;
  for (unsigned i=0; i<idsize; i++) {
    // we're fine with filled entries
    // XXX: we could still check this
    if(_fingers.valid(i))
      continue;

    //
    // Node claims there is no node to satisfy this entry in the finger table.
    // Check whether that is true.
    //

    // On every iteration we add another bit.  lower_mask looks like 000...111,
    // but we use it as 111...000 by ~-ing it.
    if(i)
      lower_mask |= (1<<(i-1));

    // flip the bit, and turn all bits to the right of the flipped bit into
    // zeroes.
    NodeID lower = _id ^ (1<<i);
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
      KDEBUG(5) << "not stabilized because node with ID " << printbits(_id) << ", entry " << i << " is invalid." << endl;
      KDEBUG(5) << "lowermask = " << printbits(lower_mask) << endl;
      KDEBUG(5) << "~lowermask = " << printbits(~lower_mask) << endl;
      KDEBUG(5) << "lower = " << printbits(lower) << endl;
      KDEBUG(5) << "upper = " << printbits(upper) << endl;
      KDEBUG(5) << "existing = " << printbits(*it) << endl;
      return false;
    }
  }

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
  la.id = _id;
  la.ip = ip();
  la.key = _id;
  KDEBUG(2) << "join: lookup my id" << endl;
  doRPC(wkn, &Kademlia::do_lookup, &la, &lr);
  KDEBUG(2) << "join: lookup my id: node " << printID(lr.id) << endl;

  // put well known dude in finger table
  NodeID succ = lr.rid;
  IPAddress succip = lr.ip;

  unsigned entry = merge_into_ftable(succ, wkn);
  KDEBUG(3) << "join: inserted at entry " << entry << endl;

  // all entries further away than him need to be refereshed.
  // see section 2.3
  for(unsigned i=entry+1; i<idsize; i++) {
    // always look up on the previous node we learned about.
    la.key = (_id ^ (1<<i));
    KDEBUG(3) << "join: looking up entry " << i << ": " << printbits(la.key) << endl;
    doRPC(lr.ip, &Kademlia::do_lookup, &la, &lr);
    KDEBUG(3) << "join: looking result for entry " << i << ": " << printbits(lr.id) << endl;
    if(lr.id != _id)
      merge_into_ftable(la.key, lr.ip);
  }

  // now get the keys from our successor
  transfer_args ta;
  transfer_result tr;
  ta.id = _id;
  ta.ip = ip();
  KDEBUG(2) << "join: Node " << printbits(_id) << " initiating transfer from " << printbits(lr.id) << endl;
  doRPC(succip, &Kademlia::do_transfer, &ta, &tr);

  // merge that data in our _values table
  for(map<NodeID, Value>::const_iterator pos = tr.values.begin(); pos != tr.values.end(); ++pos)
    _values[pos->first] = pos->second;
}

void
Kademlia::do_join(void *args, void *result)
{
  join_args *jargs = (join_args*) args;
  KDEBUG(2) << "do_join " << printbits(jargs->id) << " entering\n";
  merge_into_ftable(jargs->id, jargs->ip);
}


void
Kademlia::do_transfer(void *args, void *result)
{
  transfer_args *targs = (transfer_args*) args;
  transfer_result *tresult = (transfer_result*) result;
  merge_into_ftable(targs->id, targs->ip);

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


void
Kademlia::do_lookup(void *args, void *result)
{
  lookup_args *largs = (lookup_args*) args;
  lookup_result *lresult = (lookup_result*) result;
  KDEBUG(3) << "do_lookup: id = " << printbits(largs->id) << ", ip = " << largs->ip << ", key = " << printbits(largs->key) << endl;

  NodeID origID = largs->id;
  IPAddress origIP = largs->ip;

  NodeID bestID = _id;
  NodeID bestdist = distance(_id, largs->key);

  // XXX: very inefficient
  for(unsigned i=0; i<idsize; i++) {
    KDEBUG(3) << "do_lookup: " << printbits(largs->key) << " -> [" << i << "] = ";
    
    if(!_fingers.valid(i)) {
      DEBUG(3) << "invalid" << endl;
      continue;
    } else {
      DEBUG(3) << printbits(_fingers.get_id(i)) << " (dist = " << printbits(distance(_fingers.get_id(i), largs->key)) << ")" << endl;
    }

    NodeID dist;
    if((dist = distance(_fingers.get_id(i), largs->key)) < bestdist) {
      bestdist = dist;
      bestID = _fingers.get_id(i);
    }
  }
  KDEBUG(2) << "do_lookup: result is key: " << printbits(bestID) << endl;

  // if this is us, then reply
  if(bestID == _id) {
    lresult->id = bestID;
    lresult->ip = ip();
    KDEBUG(2) << "do_lookup: I am the best match for " << printID(largs->key) << endl;
    goto done;
  }

  // otherwise do the lookup call to whomever we think is best
  KDEBUG(2) << "do_lookup: *** recursive lookup(" << printbits(largs->key) << ") @ " << printbits(bestID) << " (ip = " << _fingers.get_ipbyid(bestID) << ")" << endl;
  largs->id = _id;
  largs->ip = ip();
  doRPC(_fingers.get_ipbyid(bestID), &Kademlia::do_lookup, args, result);

done:
  // only merge _after_ the lookup to avoid returning a node's own id.
  merge_into_ftable(origID, origIP);

  // put my own id in reply
  lresult->rid = _id;
}



unsigned
Kademlia::merge_into_ftable(NodeID id, IPAddress ip)
{
  KDEBUG(3) << "merge_into_ftable: id = " << printbits(id) << ", ip = " << ip << endl;
  assert(id != _id);

  unsigned called = 0;
  unsigned entry = 0;
  for(unsigned i=0; i<idsize; i++) {
    if(flipbitandmaskright(_id, i) == maskright(id, i)) {
      KDEBUG(4) << "merge_into_ftable: flipbitandmaskright(_id, " << i << ") == " << printbits(flipbitandmaskright(_id, i)) << endl;
      KDEBUG(4) << "merge_into_ftable:           maskright(id,  " << i << ") == " << printbits(maskright(id, i)) << endl;
      KDEBUG(3) << "merge_into_ftable: setting to entry " << i << endl;
      _fingers.set(i, id, ip);
      called++;
      entry = i;
    }
  }
  assert(called <= 1);

  if(verbose >= 4)
    dump();
  return entry;
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
    sprintf(&(buf[j++]), "%u", (char) (id >> i) & 0x1);
  // sprintf(&(buf[j]), ":%llx", id);
  sprintf(&(buf[j]), ":%hx", (char) id);

  return string(buf);
}


string
Kademlia::printID(NodeID id)
{
  char buf[128];
  sprintf(buf, "%hx", id);
  return string(buf);
}



Kademlia::NodeID
Kademlia::distance(Kademlia::NodeID from, Kademlia::NodeID to)
{
  // KDEBUG(5) << "distance between " << printbits(from) << " and " << printbits(to) << " = ";
  NodeID ret;

  ret = from ^ to;

  DEBUG(5) << printbits(ret) << "\n";
  return ret;
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
  merge_into_ftable(iargs->id, iargs->ip);

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
