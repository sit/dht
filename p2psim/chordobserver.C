#include "chordobserver.h"
#include "protocol.h"
#include "args.h"
#include "network.h"
#include <iostream>
#include <list>
#include <algorithm>
#include <stdio.h>

#include "chord.h"
#include "koorde.h"

using namespace std;

ChordObserver* ChordObserver::_instance = 0;

ChordObserver*
ChordObserver::Instance(Args *a)
{
  if(!_instance)
    _instance = new ChordObserver(a);
  return _instance;
}


ChordObserver::ChordObserver(Args *a)
{
  _reschedule = 0;
  _reschedule = atoi((*a)["reschedule"].c_str());
  _type = (*a)["type"];
  _num_nodes = atoi((*a)["numnodes"].c_str());
  assert(_num_nodes > 0);

  _init_num = atoi((*a)["initnodes"].c_str());
  if (_init_num > 0) {
    init_nodes(_init_num);
  }
  lid.clear();
}

ChordObserver::~ChordObserver()
{
}

void
ChordObserver::init_nodes(unsigned int num)
{
  list<Protocol*> l = Network::Instance()->getallprotocols(_type);
  list<Protocol*>::iterator pos;

  vector<Chord::IDMap> ids;
  Chord *c;
  Chord::IDMap n;
  unsigned int i = 0;

  for (pos = l.begin(); pos != l.end(); ++pos) {
    c = (Chord *)(*pos);
    assert(c);
    n.ip = c->node()->ip();
    n.id = c->id();
    ids.push_back(n);

    if (++i == num) {
      break;
    }
  }

  sort(ids.begin(),ids.end(),Chord::IDMap::cmp);
  i = 0;
  for (pos = l.begin(); pos != l.end(); ++pos) {
    c = (Chord *)(*pos);
    assert(c); 
    c->init_state(ids);
    if (++i == num) break;

  }
}

void
ChordObserver::execute()
{
  list<Protocol*> l = Network::Instance()->getallprotocols(_type);
  list<Protocol*>::iterator pos;

  //i only want to sort it once after all nodes have joined! 
  Chord *c = 0;
  if (lid.size() != _num_nodes) {
    lid.clear();
    for (pos = l.begin(); pos != l.end(); ++pos) {
      c = (Chord *)(*pos);
      assert(c);
      lid.push_back(c->id ());
    }

    sort(lid.begin(), lid.end());

    vector<ConsistentHash::CHID>::iterator i;
    printf ("sorted nodes %d %d\n", lid.size (), _num_nodes);
    for (i = lid.begin (); i != lid.end() ; ++i) {
      printf ("%qx\n", *i);
    }
  }

  for (pos = l.begin(); pos != l.end(); ++pos) {
    c = (Chord *)(*pos);
    assert(c);
    if (!c->stabilized(lid)) {
      cout << now() << " NOT STABILIZED" << endl;
      if (_reschedule > 0) reschedule(_reschedule);
      return;
    }

  }

  cout << now() << " STABILIZED" << endl;
  cout << now() << " CHORD NODE STATS" << endl;
  for (pos = l.begin(); pos != l.end(); ++pos) {
    assert(c);
    c = (Chord *)(*pos);
    c->dump();
  }
}
