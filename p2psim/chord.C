#include "chord.h"
#include "node.h"
#include "packet.h"
#include <stdio.h>
#include <iostream>

using namespace std;

/********* id_util functions *************/

// Is n between a and b?
// a < n <= b
static bool
between(HashedID a, HashedID b, HashedID n)
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

Chord::Chord(Node *n)
  : Protocol(n)
{
  me.hid = rand(); //for now, a rand() ID is as good as hashe(IP)
  me.id = n->id();

  predecessor.id = 0;
  successor = me;
}

Chord::~Chord()
{
}

string
Chord::s()
{
  char buf[50];
  sprintf(buf, "Chord(%u,%u)", me.id, me.hid);
  return string(buf);
}

void *
Chord::find_successor_x(void *x)
{
  printf("Chord(%u,%u)::find_successor_x(%u)\n",
         me.id, me.hid, (int) x);
  return (void *) 99;
}

// External event that tells a node to contact the well-known node
// and try to join.
// XXX assumes that well-known node has address 1.
void
Chord::join(void *)
{
  cout << s() + "::join" << endl;
  void *ret = doRPC((IPAddress) 1, Chord::find_successor_x, me.hid);
  printf("Chord(%u,%u)::join2 %u\n",
         me.id, me.hid, (int) ret);
  // stabilize();
}

#if 0
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
