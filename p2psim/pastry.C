#include "pastry.h"
#include "packet.h"
#include <iostream>
#include <algorithm>
#include "p2psim.h"
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
  NodeID mask = 0;
  for(unsigned i=0; i<idlength; i++) {
    mask = ((NodeID) 1) << (idlength-i-1);
    if((n & mask) != (m & mask))
      return i;
  }
  return idlength;
}

// returns the value of digit d in n, given base 2^_b
//
// The part before the & considers the number to be divided in chunks of _b
// bits.  Since we want the d'th chunk (counted from the left) we shift to the
// right until the chunk we want is on the right side of the number.
//
// The part after the & simply creates a mask of _b 1's to mask out the chunks
// we don't want.
unsigned
Pastry::get_digit(NodeID n, unsigned d)
{
  return (n >> (idlength-_b*d)) & ((((NodeID) 1) << _b) - 1);
}


//
// Routing algorithm from Table 1 in Pastry paper.
//
void
Pastry::route(NodeID *D, void *msg)
{
  IPAddress nexthop = 0;

  // in leaf set?
  LS::iterator pos;
  if((*(pos = min_element(_lleafset.begin(), _lleafset.end()))) <= *D &&
     (*(pos = max_element(_hleafset.begin(), _lleafset.end()))) > *D)
  {
    RTEntrySmallestDiff lsd = for_each(_lleafset.begin(), _lleafset.end(), RTEntrySmallestDiff(*D));
    RTEntrySmallestDiff usd = for_each(_hleafset.begin(), _hleafset.end(), RTEntrySmallestDiff(*D));
    nexthop = lsd.diff < usd.diff ? lsd.ip : usd.ip;
  }
  
  // not in leaf set?  try the routing table.
  if(!nexthop) {
    unsigned l = shared_prefix_len(*D, _id);
    IPAddress ip;
    if((ip = _rtable[get_digit(*D, l)][l].second)) {
      nexthop = ip;


    // not in routing table either?
    // find any D that
    // shared_prefix_len(T, D) >= l
    // |T - *D| < |A - *D|
    } else {
      // search in lleafset
      for(ES::const_iterator pos = _lleafset.begin(); pos != _lleafset.end(); ++pos) {
        NodeID T = (*pos).first;
        if((shared_prefix_len((*pos).first, *D) >= l) && (abs(T - *D) < abs(_id - *D))) {
          nexthop = (*pos).second;
          goto done;
        }
      }

      // search in uleafset
      for(ES::const_iterator pos = _uleafset.begin(); pos != _uleafset.end(); ++pos) {
        NodeID T = (*pos).first;
        if((shared_prefix_len((*pos).first, *D) >= l) && (abs(T - *D) < abs(_id - *D))) {
          nexthop = (*pos).second;
          goto done;
        }
      }

      // search in routing table
      for(ES::const_iterator pos = _uleafset.begin(); pos != _uleafset.end(); ++pos) {
        NodeID T = (*pos).first;
        if((shared_prefix_len((*pos).first, *D) >= l) && (abs(T - *D) < abs(_id - *D))) {
          nexthop = (*pos).second;
          goto done;
        }
      }
    }
  }

done:
  doRPC(nexthop, &Pastry::route, D, (void*)0);
}
