#include "chord.h"
#include <iostream>
using namespace std;

#if 0

/********* id_util functions *************/
static bool
between(HashedID me, HashedID x, HashedID succ)
{
  bool r;
  if (a == b) {
    r = (n!=a);
  }else if (a < b) {
    r = (n > a) && (n < b);
  }else {
    r = (n > a) || (n < b);
  }
  return r;
}

Chord::Chord()
{
  me.hID = rand(); //for now, a rand() ID is as good as hashe(IP)
  me.ID = getNodeID();

}

Chord::~Chord()
{
}

void
Chord::run()
{
  cout << "Chord running" << endl;
}

void
Chord::create()
{
  predecessor.id = 0;
  successor = me;
}

void
Chord::join(NodeID bnode)
{
  predecessor.id = 0; //0 means null
  successor = doRPC(bnode, find_successor, myNodeID); //equivalent to bnode->lookup(myNodeID, SUCC_NUM);
  stabilize();
}

IDMap
Chord::find_successor(HashedID n, bool recursive)
{
  //how to do recursive queries with RPC? 
  IDMap nn;

  if (recursive) {
    nn = next(n);
    if (nn.id == me.id) {
      return me;
    }else{
      return doRPC(successor.id, lookup, n, recursive);
    }
  }else{
    cn = me;
    nn = next(n);
    while (nn.id != cn.id) {
      cn = nn;
      nn = doRPC(cn.id, lookup, n, recursive);
    }
    return cn;
  } 
}

void
Chord::stabilize()
{
  IDMap x = doRPC(successor.id, get_predecessor);
  if (between(me.hid, successor.id, x.id)) {
    successor = x;
  }
  doRPC(successor.id, notify, me);

  //in chord pseudocode, fig 6 of ToN paper, this is a separate periodically called function
  check_predecessor();
}

void
Chord::check_predecessor()
{
  if (Failed(predecessor.id)) {
    predecessor.id = 0;
  }
}

void
Chord::notify(IDMap nn)
{
  if ((predecessor.id == 0) || between(predecessor.hid, me.hid, nn.hid)) {
    predecessor = nn;
  }
}

IDMap
Chord::get_predecessor()
{
  return predecessor;
}

IDMap
Chord::next(HashedID n)
{
  if (between(predecessor.hid, me.hid, n)) { 
    return me;
  }else {
    return successor;
  }
}

void
Chord::leave()
{
}

void
Chord::crash()
{
  //suspend the currently running thread?
}

void
Chord::insert_doc(HashedID docID) 
{
  IDMap n = find_successor(docID);
  doRPC(n.id, store_doc, docID);
  
}

void
Chord::store_doc(docID)
{ 
  /* there should be a generic class for handling doc store/retrieve locally */
}

void
Chord::lookup_doc(HashedID docID) 
{
}

#endif
