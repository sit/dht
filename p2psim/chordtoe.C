#include "chordtoe.h"
#include <stdio.h>
#include <iostream>
#include <algorithm>
#include "network.h"
#include "topology.h"

using namespace std;

Chord::IDMap 
LocTableToe::next_hop(Chord::CHID key, bool *done) 
{
  idmapwrap *e = ring.first ();
  Topology *t = Network::Instance()->gettopology ();
  idmapwrap *b = NULL;
  int best = RAND_MAX;

  while (e) {
    //    cerr << "trying: " << e->n.id << " for [" << me.id << ", " << key << "]\n";
    if (ConsistentHash::betweenrightincl(me.id, key, e->n.id)) {
      int lat = t->latency (e->n.ip, me.ip);
      if (lat < best) {
	best = lat;
	b = e;
	//	cerr << key << " --- new best: " << b->n.ip << " w lat " << lat << "\n";
      } 
    }
    e = ring.next (e);
    //if (e) cerr << "next_hop for " << me.ip << " / " << key << " looking at " << e->n.ip << "\n";
  }

  assert (b);
  Chord::IDMap ret = b->n;
  
  assert (ret.ip != me.ip);
  assert (ConsistentHash::betweenrightincl(me.id, key, ret.id));

  *done = false;
  return ret;
}

ChordToe::ChordToe (Node *n, Args &a) : 
  ChordFinger (n, a, New LocTableToe ()),  _numtoes (16)
{
};


int
ChordToe::add_toe (IDMap id)
{
  _toes.push_back (id);

  if (_toes.size () <= _numtoes) return 1000000;

  Topology *t = Network::Instance()->gettopology ();

  int worstlat = t->latency(me.ip, _toes[0].ip);
  vector<IDMap>::iterator i;
  vector<IDMap>::iterator worst = _toes.begin ();
  for (i = _toes.begin (); i != _toes.end (); ++i)
    {
      IDMap cur = *i;
      int lat = t->latency (me.ip, cur.ip);
      if (lat > worstlat) 
	{
	  worstlat = lat;
	  worst = i;
	}
    }
  
  _toes.erase (worst);
  return worstlat;
}

void
ChordToe::init_state (vector<IDMap> ids) 
{
  loctable->set_evict (false);

  int worst_lat = 1000000; //timeout is a good max

  Topology *t = Network::Instance()->gettopology ();

  //search for the best toes 
  for (uint i = 0; i < ids.size(); i++)
    {
      int lat = t->latency (me.ip, ids[i].ip);
      if (lat < worst_lat || _toes.size () < _numtoes) 
	worst_lat = add_toe (ids[i]);
    }

  //add the toes
  for (uint i = 0; i < _toes.size (); i++)
    loctable->add_node (_toes[i]);

  //  Chord::init_state (ids);
  ChordFinger::init_state (ids);
}


