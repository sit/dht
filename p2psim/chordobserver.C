#include "chordobserver.h"
#include "protocol.h"
#include "args.h"
#include "network.h"
#include <iostream>
#include <list>
#include <algorithm>
#include <stdio.h>


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
  if (!a)  {
    cout << now() << "ChordObserver created WRONGLY!" << endl;
    exit(1);
  }
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
  _allsorted.clear();
}

ChordObserver::~ChordObserver()
{
}

/*
 * if max = 0 or _num_nodes, then it returns all sorted nodes,
 otherwise, return max number of nodes */

vector<Chord::IDMap>
ChordObserver::get_sorted_nodes(unsigned int max)
{
  if ((!max || (max == _num_nodes)) && _allsorted.size() > 0) {
    return _allsorted;
  }

  list<Protocol*> l = Network::Instance()->getallprotocols(_type);
  list<Protocol*>::iterator pos;

  vector<Chord::IDMap> ids;
  ids.clear();
  Chord *c;
  Chord::IDMap n;
  unsigned int i = 0;

  for (pos = l.begin(); pos != l.end(); ++pos) {
    c = (Chord *)(*pos);
    assert(c);
    n.ip = c->node()->ip();
    n.id = c->id();
    ids.push_back(n);

    if (++i == max) {
      break;
    }
  }

  sort(ids.begin(),ids.end(),Chord::IDMap::cmp);

  if ((!max)|| (max == _num_nodes)) {
    _allsorted = ids;
  }
  return ids;
}

void
ChordObserver::init_nodes(unsigned int num)
{
  vector<Chord::IDMap> ids;

  list<Protocol*> l = Network::Instance()->getallprotocols(_type);
  list<Protocol*>::iterator pos;
  Chord *c;
  
  ids = get_sorted_nodes(num);
  unsigned int i = 0;
  for (pos = l.begin(); pos != l.end(); ++pos) {
    c = (Chord *)(*pos);
    assert(c); 
    c->init_state(ids);
    if (++i == num) break;
  }
  printf("ChordObserver finished initing %d nodes\n", num);
  for (uint i = 0; i < ids.size(); i++) {
    printf("%qx %u\n", ids[i].id, ids[i].ip);
  }
}

void
ChordObserver::execute()
{
  if (!_reschedule) {
    return;
  }

  list<Protocol*> l = Network::Instance()->getallprotocols(_type);
  list<Protocol*>::iterator pos;

  vivaldi_error ();
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

    // vector<ConsistentHash::CHID>::iterator i;
    printf ("sorted nodes %d %d\n", lid.size (), _num_nodes);
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

void
ChordObserver::vivaldi_error()
{
  Topology *t = (Network::Instance()->gettopology());
  assert (t);
  vector<double> avg_errs;

  list<Protocol*> l = Network::Instance()->getallprotocols(_type);
  list<Protocol*>::iterator outer, inner; 
  for (outer = l.begin(); outer != l.end(); ++outer) {
    double sum = 0;
    uint sum_sz = 0;
    Chord *c = (Chord *)(*outer);
    assert (c);
    if (!c->_vivaldi) continue;

    cout << "COORD " <<  c->id() << " " << now () << ": ";
    Vivaldi::Coord vc = c->get_coords();
    for (uint j = 0; j < vc._v.size(); j++)
      cout << vc._v[j] << " ";

    cout << "\n";
    for (inner = l.begin(); inner != l.end(); ++inner) {
      Chord *h = (Chord *)(*inner);
      assert (h);
      if (!h->_vivaldi) continue;
      Vivaldi::Coord vc1 = h->get_coords ();
      double vd = dist(vc, vc1);
      double rd = t->latency(c->node()->ip(), h->node()->ip());
      if (rd > 0.0 && vd > 0.0) {
	//	cout << c->id () << " to " << h->id () << ". Predicted: " << 
	//vd << " real latency was " << rd << "\n";

	sum += fabs(vd - rd);
	sum_sz++;
      }
      
    }
    if (sum_sz)
      cout << now() << " average error for " << c->id () << ": " << sum/sum_sz 
      	   << " after " << c->_vivaldi->nsamples() << endl;
      avg_errs.push_back (sum/sum_sz);
  }

  if (avg_errs.size() > 0) {
    sort (avg_errs.begin(), avg_errs.end());
    
    cout << " vivaldi median error: " << avg_errs[avg_errs.size() / 2] << "\n";
  }
}
