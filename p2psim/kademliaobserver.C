#include "kademliaobserver.h"
#include "protocol.h"
#include "args.h"
#include "network.h"
#include <iostream>
#include <list>
#include <algorithm>
#include <stdio.h>

#include "kademlia.h"

using namespace std;

KademliaObserver* KademliaObserver::_instance = 0;

KademliaObserver*
KademliaObserver::Instance(Args *a)
{
  if(!_instance)
    _instance = new KademliaObserver(a);
  return _instance;
}


KademliaObserver::KademliaObserver(Args *a)
{
  _reschedule = 0;
  _reschedule = atoi((*a)["reschedule"].c_str());
  _type = (*a)["type"];
  _num_nodes = atoi((*a)["numnodes"].c_str());
  assert(_num_nodes > 0);
  lid.clear();
}

KademliaObserver::~KademliaObserver()
{
}

void
KademliaObserver::execute()
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
    if (!c->stabilized(lid)) {
      cout << now() << " NOT STABILIZED" << endl;
      if (_reschedule > 0)
        reschedule(_reschedule);
      return;
    }

  }

  cout << now() << " STABILIZED" << endl;
  cout << now() << " Kademlia finger tables" << endl;
  for (pos = l.begin(); pos != l.end(); ++pos) {
    assert(c);
    c = (Kademlia *)(*pos);
    c->dump();
  }
}
