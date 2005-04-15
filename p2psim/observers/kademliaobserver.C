/*
 * Copyright (c) 2003-2005 Thomer M. Gil
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
  // register as an observer of all Kadmelia instances
  const set<Node*> *l = Network::Instance()->getallnodes();
  for(set<Node*>::iterator pos = l->begin(); pos != l->end(); ++pos) {
    Kademlia *k = (Kademlia*) *pos;
    k->registerObserver(this);
    DEBUG(1) << "KademliaObserver registered with " << k->id() << endl;
  }
}


KademliaObserver::~KademliaObserver()
{
}


void
KademliaObserver::kick(Observed *, ObserverInfo *)
{
  stabilized();
}

bool
KademliaObserver::stabilized()
{
  const static set<Node*> *allprotos = 0;
  set<Node*> liveprotos;
  vector<Kademlia::NodeID> liveIDs;

  if(!allprotos)
    allprotos = Network::Instance()->getallnodes();

  for(set<Node*>::const_iterator i = allprotos->begin(); i != allprotos->end(); ++i) {
    Kademlia *k = (Kademlia *)(*i);
    if(k->alive()) {
      liveprotos.insert(*i);
      liveIDs.push_back(k->id());
    }
  }

  for(set<Node*>::const_iterator i = liveprotos.begin(); i != liveprotos.end(); ++i) {
    Kademlia *k = (Kademlia *)(*i);
    if (!k->stabilized(&liveIDs)) {
      DEBUG(1) << now() << " NOT STABILIZED" << endl;
      return false;
    }
  }

  DEBUG(1) << now() << " STABILIZED" << endl;
  return true;
}
