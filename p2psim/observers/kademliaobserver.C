/*
 * Copyright (c) 2003 Thomer M. Gil
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

#include "kademliaobserver.h"
#include "p2psim/network.h"
#include <iostream>
using namespace std;

KademliaObserver::KademliaObserver(Args *a) : _type("Kademlia")
{
  _nnodes = atoi((*a)["nodes"].c_str());
  assert(_nnodes > 0);

  _initstate = atoi((*a)["initstate"].c_str()) ? true : false;

  // register as an observer of all Kadmelia instances
  list<Protocol*> l = Network::Instance()->getallprotocols(_type);
  for(list<Protocol*>::iterator pos = l.begin(); pos != l.end(); ++pos) {
    Kademlia *k = (Kademlia*) *pos;
    k->registerObserver(this);
    DEBUG(1) << "KademliaObserver registered with " << k->id() << endl;
  }
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
KademliaObserver::kick(Observed *, ObserverInfo *)
{
  if(_initstate) {
    init_state();
    _initstate = false;
  }

  stabilized();
}

bool
KademliaObserver::stabilized()
{
  list<Protocol*> allprotos = Network::Instance()->getallprotocols(_type);
  list<Protocol*> liveprotos;
  vector<Kademlia::NodeID> liveIDs;

  for(list<Protocol*>::const_iterator i = allprotos.begin(); i != allprotos.end(); ++i) {
    Kademlia *k = (Kademlia *)(*i);
    if(k->node()->alive()) {
      liveprotos.push_back(*i);
      liveIDs.push_back(k->id());
    }
  }


  for(list<Protocol*>::const_iterator i = liveprotos.begin(); i != liveprotos.end(); ++i) {
    Kademlia *k = (Kademlia *)(*i);
    if (!k->stabilized(&liveIDs)) {
      DEBUG(1) << now() << " NOT STABILIZED" << endl;
      return false;
    }
  }

  cout << now() << " STABILIZED" << endl;
  return true;
}
