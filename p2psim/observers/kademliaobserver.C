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
 */

#include "kademliaobserver.h"
#include <iostream>
using namespace std;

KademliaObserver::KademliaObserver(Args *a) : _type("Kademlia")
{
  _num_nodes = atoi((*a)["numnodes"].c_str());
  assert(_num_nodes > 0);

  _init_num = atoi((*a)["initnodes"].c_str());
  lid.clear();
}


KademliaObserver::~KademliaObserver()
{
}


void
KademliaObserver::init_state()
{
  list<Protocol*> l = Network::Instance()->getallprotocols(_type);
  for(list<Protocol*>::iterator pos = l.begin(); pos != l.end(); ++pos) {
    Kademlia *k = (Kademlia*) *pos;
    DEBUG(1) << "KademliaObserver::init_state " << now() << endl;
    k->init_state(l);
  }
}


void
KademliaObserver::kick()
{
  if(_init_num) {
    init_state();
    _init_num = 0;
  }
}

bool
KademliaObserver::stabilized()
{
  list<Protocol*> l = Network::Instance()->getallprotocols(_type);
  list<Protocol*>::iterator pos;

  //i only want to sort it once after all nodes have joined! 
  Kademlia *c = 0;
  if (lid.size() != _num_nodes) {
    lid.clear();
    for (pos = l.begin(); pos != l.end(); ++pos) {
      c = (Kademlia *)(*pos);
      assert(c);
      lid.push_back(c->id ());
    }

    sort(lid.begin(), lid.end());

    // vector<Kademlia::NodeID>::iterator i;
    // printf ("sorted nodes %d %d\n", lid.size (), _num_nodes);
    // for (i = lid.begin (); i != lid.end() ; ++i)
    //   printf ("%hx\n", *i);
  }

  for (pos = l.begin(); pos != l.end(); ++pos) {
    c = (Kademlia *)(*pos);
    assert(c);
    c->dump();
    if (!c->stabilized(lid)) {
      DEBUG(1) << now() << " NOT STABILIZED" << endl;
      return false;
    }
  }

  DEBUG(1) << now() << " STABILIZED" << endl;
  DEBUG(1) << now() << " Kademlia finger tables" << endl;
  for (pos = l.begin(); pos != l.end(); ++pos) {
    assert(c);
    c = (Kademlia *)(*pos);
    c->dump();
  }
  return true;
}
