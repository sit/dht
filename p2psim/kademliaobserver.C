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
