#include "kademliaobserver.h"
#include <iostream>
using namespace std;

KademliaObserver* KademliaObserver::_instance = 0;

KademliaObserver*
KademliaObserver::Instance(Args *a)
{
  if(!_instance)
    _instance = New KademliaObserver(a);
  return _instance;
}


KademliaObserver::KademliaObserver(Args *a) : Observer(a)
{
  _reschedule = 0;
  _reschedule = atoi((*a)["reschedule"].c_str());

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
    k->init_state(l);
  }
}


void
KademliaObserver::execute()
{
  if(_init_num) {
    init_state();
    _init_num = 0;
  }

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
      if (_reschedule > 0)
        reschedule(_reschedule);
      return;
    }

  }

  DEBUG(1) << now() << " STABILIZED" << endl;
  DEBUG(1) << now() << " Kademlia finger tables" << endl;
  for (pos = l.begin(); pos != l.end(); ++pos) {
    assert(c);
    c = (Kademlia *)(*pos);
    c->dump();
  }
}
