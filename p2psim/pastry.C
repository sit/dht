#include "p2psim.h"
#include "pastry.h"
#include "packet.h"
#include <iostream>
using namespace std;

Pastry::Pastry(Node *n) : Protocol(n),
  // suggested values in paper
  idlength(128),
  _b(4),
  _L(2 << _b),
  _M(2 << _b)
{
  // create a random 128-bit ID
  _id = BN_new();
  BN_init(_id);
  BN_pseudo_rand(_id, idlength, -1, 0);
  cout << "Pastry id = " << BN_bn2hex(_id) << endl;
}


Pastry::~Pastry()
{
  BN_free(_id);
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
  for(int i=0; i<idlength; i++)
    if(BN_is_bit_set(n, i) != BN_is_bit_set(m, i))
      return i;
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
  NodeID n = BN_new();
  BN_rshift(n, nx, (idlength - _b*d)); // n = n << (idlength-b*d)
  BN_mask_bits(n, _b);                 // n |= b

  unsigned r = 0;
  for(int i=0; i<_b; i++)
    if(BN_is_bit_set(n, i))
      r |= 1<<i;
  BN_free(n);
  return r;
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
    doRPC(nexthop, &Pastry::route, D, (void*)0);
    return;
  }

  // handle the so-called rare case.
}
