#include "kademlia.h"
#include "packet.h"
#include "nodefactory.h"
#include <iostream>
#include <stdio.h>
#include "p2psim.h"
#include <stdio.h>
#include <algorithm>
using namespace std;


Kademlia::Kademlia(Node *n) : Protocol(n)
{
  // _id = (((NodeID) random()) << 32) | random();
  // printf("id = %llx\n", _id);
  _id = (NodeID) random();

  KDEBUG(1) << "constructor: " << printbits(_id) << endl;
  _values.clear();
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
      lower_mask |= (1<<i-1);

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
      cout << "not stabilized because node with ID " << printbits(_id) << ", entry " << i << " is invalid." << endl;
      cout << "lowermask = " << printbits(lower_mask) << endl;
      cout << "~lowermask = " << printbits(~lower_mask) << endl;
      cout << "lower = " << printbits(lower) << endl;
      cout << "upper = " << printbits(upper) << endl;
      cout << "existing = " << printbits(*it) << endl;
      return false;
    }
  }

  return true;
}


void
Kademlia::join(Args *args)
{
  // _id = args->nget<NodeID>("bitkey", 2);
  // KDEBUG(1) << "Node ID = " << printID(_id) << endl;

  IPAddress wkn = args->nget<IPAddress>("wellknown");
  if(wkn == ip()) {
    KDEBUG(1) << "Node " << printID(_id) << " is wellknown." << endl;
    return;
  }

  // add myself in finger table
  for(unsigned i=0; i<idsize; i++)
    _fingers.set(i, _id, ip());

  // lookup my own key with well known node.
  lookup_args la;
  lookup_result lr;
  la.key = _id;
  KDEBUG(1) << "Doing lookup for my ID." << endl;
  doRPC(wkn, &Kademlia::do_lookup, &la, &lr);
  KDEBUG(1) << "Result of lookup for " << printID(_id) << " is " << printID(lr.id) << ", which is node " << lr.ip << endl;

  // now we know the closest node in our ID space.  add him to our finger table.
  handle_join(lr.id, lr.ip);

  // ...and get his data
  transfer_args ta;
  transfer_result tr;
  ta.id = _id;
  KDEBUG(1) << "Node " << printID(_id) << " initiating transfer." << endl;
  doRPC(lr.ip, &Kademlia::do_transfer, &ta, &tr);

  // merge that data in our _values table
  for(map<NodeID, Value>::const_iterator pos = tr.values.begin(); pos != tr.values.end(); ++pos)
    _values[pos->first] = pos->second;

  KDEBUG(1) << "Transfer done." << endl;

  join_args ja;
  join_result jr;
  ja.id = _id;
  ja.ip = ip();

  // why are we using wkn for the first lookup?
  IPAddress ip = wkn;
  for(unsigned i=0; i<idsize; i++) {
    NodeID key = _id ^ (1<<i);
    // now tell all the relevant nodes about me.
    KDEBUG(2) << "*** Iteration " << i << ".  Doing lookup for " << printbits(key) << " to join\n";

    lookup_args la;
    lookup_result lr;

    la.key = key;
    do_lookup(&la, &lr);

    // send a join request to that guy
    ip = lr.ip;
    if(!doRPC(ip, &Kademlia::do_join, &ja, &jr))
      _fingers.unset(i);
    else
      _fingers.set(i, lr.id, ip);
  }
}


void
Kademlia::do_join(void *args, void *result)
{
  join_args *jargs = (join_args*) args;

  KDEBUG(1) << "do_join " << printbits(jargs->id) << " entering\n";
  handle_join(jargs->id, jargs->ip);
}


void
Kademlia::do_transfer(void *args, void *result)
{
  transfer_args *targs = (transfer_args*) args;
  transfer_result *tresult = (transfer_result*) result;

  KDEBUG(1) << "handle_transfer to node " << printID(targs->id) << "\n";
  if(_values.size() == 0) {
    KDEBUG(1) << "handle_transfer_cb; no values: done!\n";
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

  NodeID bestID = _id;
  NodeID bestdist = distance(_id, largs->key);
  KDEBUG(3) << "do_lookup for key " << printID(largs->key) << ", bestID = " << printID(bestID) << ", bestdist =  " << printID(bestdist) << "\n";

  // XXX: very inefficient
  for(unsigned i=0; i<idsize; i++) {
    KDEBUG(3) << "do_lookup, considering _fingers[" << i << "], key: " << printID(_fingers.get_id(i)) << "\n";
    if(!_fingers.valid(i)) {
      KDEBUG(3) << "entry " << i << " is invalid\n";
      continue;
    }

    NodeID dist;
    if((dist = distance(_fingers.get_id(i), largs->key)) < bestdist) {
      bestdist = dist;
      bestID = _fingers.get_id(i);
    }
  }
  KDEBUG(2) << "do_lookup, result is key: " << printbits(bestID) << ", distance = " << printbits(bestdist) << "\n";

  // if this is us, then reply
  if(bestID == _id) {
    lresult->id = bestID;
    lresult->ip = ip();
    KDEBUG(2) << "I (" << printID(_id) << ") am the best match for " << printID(largs->key) << endl;
    return;
  }

  // otherwise do the lookup call to whomever we think is best
  doRPC(_fingers.get_ipbyid(bestID), &Kademlia::do_lookup, args, result);
}



void
Kademlia::handle_join(NodeID id, IPAddress ip)
{
  KDEBUG(2) << "handle_join (myid = " << printID(_id) << "), id = " << printID(id) << ", ip = " << ip << endl;
  for(unsigned i=0; i<idsize; i++) {
    NodeID newdist = distance(id, _id ^ (1<<i));
    NodeID curdist = distance(_fingers.get_id(i), _id ^ (1<<i));
    if(newdist < curdist) {
      KDEBUG(2) << "handle_join " << printbits(id) << " is better than old " << printbits(_fingers.get_id(i)) << " for entry " << i << "\n";
      _fingers.set(i, id, ip);
    }
  }
}

string
Kademlia::printbits(NodeID id)
{
  char buf[128];

  unsigned j=0;
  for(int i=idsize-1; i>=0; i--)
    sprintf(&(buf[j++]), "%u", (unsigned) (id >> i) & 0x1);
  // sprintf(&(buf[j]), ":%llx", id);
  sprintf(&(buf[j]), ":%hx", id);

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
  DEBUG(5) << "distance between " << printbits(from) << " and " << printbits(to) << " = ";
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

  ia.key = args->nget<NodeID>("key");
  ia.val = args->nget<Value>("val");

  KDEBUG(1) << "insert " << printID(ia.key) << ":" << ia.val << endl;
  do_insert(&ia, &ir);
}


void
Kademlia::do_insert(void *args, void *result)
{
  insert_args *iargs = (insert_args*) args;
  // insert_result *iresult = (insert_result*) result;

  lookup_args la;
  lookup_result lr;
  la.key = iargs->key;
  do_lookup(&la, &lr);

  if(lr.id == _id) {
    KDEBUG(1) << "Node " << printID(_id) << " storing " << printID(iargs->key) << ":" << iargs->val << "." << endl;
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
