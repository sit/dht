#include "pastry.h"
#include "packet.h"
#include <iostream>
#include "p2psim.h"
#include <stdlib.h>
#include <stdio.h>
using namespace std;

Pastry::Pastry(Node *n) : Protocol(n),
  // suggested values in paper
  idlength(8*sizeof(NodeID)),
  _b(4),
  _L(2 << _b),
  _M(2 << _b)
{
  _id = random();
  _id <<= 32;   // XXX: not very portable
  _id |= random();
  printf("id = %llX\n", _id);
}


Pastry::~Pastry()
{
}

void
Pastry::join(Args *a)
{
  cout << "Pastry join" << endl;
}

void
Pastry::leave(Args*)
{
  cout << "Pastry leave" << endl;
}

void
Pastry::crash(Args*)
{
  cout << "Pastry crash" << endl;
}

void
Pastry::insert(Args*)
{
  cout << "Pastry insert" << endl;
}

void
Pastry::lookup(Args *args)
{
  // if in leaf set
  cout << "Pastry lookup" << endl;
}


// length of shared prefix between two NodeID's.
unsigned
Pastry::shared_prefix_len(NodeID n, NodeID m)
{
  NodeID mask;
  for(int i=0; i<idlength; i++) {
    mask = 1 << (idlength - i);
    if(n | mask != m | mask)
      return i;
  }
  return idlength;
}

// returns the value of digit d in n, given base 2^_b
//
// basically, the strategy is to divide the bits into groups of _b bits, and
// then taking the d'th group.
//
unsigned
Pastry::get_digit(NodeID nx, unsigned d)
{
  return (nx >> (idlength-_b*d)) | _b;
}


//
// Routing algorithm from Table 1 in Pastry paper.
//
void
Pastry::route(NodeID D, void*)
{
  IPAddress nexthop;

  // if in leaf set
  //   forward to the node that is closes
  //   return;
  //

  // if it's in our routing table, forward it.
  unsigned l = shared_prefix_len(D, _id);
  if((nexthop = _rtable[get_digit(D, l)][l].second)) {
    // doRPC(nexthop, &Pastry::route, D, (void*)0);
    return;
  }

  // handle the so-called rare case.
  // forward to T \in L u R u M such that
  // shared_prefix_len(T, D) >= l
  // |T - D| < |A - D|
}
