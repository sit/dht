/*
 * Copyright (c) 2003 [NAMES_GO_HERE]
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#include "pastry.h"
#include <stdio.h>
#include <iostream>
using namespace std;

Pastry::Pastry(Node *n) : P2Protocol(n),
  // suggested values in paper
  idlength(8*sizeof(NodeID)),
  _b(4),
  _L(2 << _b),
  _M(2 << _b)
{
  assert(!(idlength % _b));
  _id = (((NodeID) random()) << 32) | random();
  printf("%d = %llX\n", ip(), _id);
}


Pastry::~Pastry()
{
}


void
Pastry::join(Args *args)
{
  IPAddress wkn = args->nget<IPAddress>("wellknown");
  if(wkn == ip())
    return;


  join_msg_args ja;
  ja.id = _id;
  ja.ip = ip();

  join_msg_ret jr;
  doRPC(wkn, &Pastry::do_join, &ja, &jr);
  printf("%llX got reply from %llX\n", _id, jr.id);

  // now merge his stuff into my routing table
  unsigned row = shared_prefix_len(_id, jr.id);
  unsigned column = get_digit(jr.id, row);
  if(!(_rtable[row][column].second)) {
    _rtable[row][column] = make_pair(jr.id, jr.ip);
  }
}


void
Pastry::do_join(join_msg_args *ja, join_msg_ret *jr)
{
  jr->id = _id;
  jr->ip = ip();
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
  for(unsigned i=0; i<idlength/_b; i++)
    if(get_digit(n, i) != get_digit(m, i))
      return i;
  return idlength/_b;
}

// returns the value of digit d in n, given base 2^_b.  b == 0 is the most
// significant digit.
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
  return (n >> (idlength-_b*(d+1))) & ((((NodeID) 1) << _b) - 1);
}


//
// Routing algorithm from Table 1 in Pastry paper.
//
void
Pastry::route(NodeID *D, void *msg)
{
  IPAddress nexthop = 0;

  // in leaf set?
  ES::iterator pos;
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
      for(ES::const_iterator pos = _hleafset.begin(); pos != _hleafset.end(); ++pos) {
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
