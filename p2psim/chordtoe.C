#include "chordtoe.h"

#define TOE_GREEDY_LAT 0
#define TOE_GREEDY_ID 1
#define TOE_NOTGREEDY_LAT 3
#define TOE_RTM 2
Chord::IDMap 
LocTableToe::next_hop(Chord::CHID key, bool *done) 
{

  Topology *t = Network::Instance()->gettopology ();

  *done = false;
  
  if (_style == TOE_GREEDY_LAT) {
    idmapwrap *e = ring.first ();
    idmapwrap *b = NULL;
    int best = RAND_MAX;

    int i = 0;
    while (e) {
      if (ConsistentHash::betweenrightincl(me.id, key, e->n.id)) {
	int lat = t->latency (me.ip, e->n.ip);
	if (lat < best) {
	  best = lat;
	  b = e;
	} 
      }
      e = ring.next (e);
      i++;
    }
    
    assert (b);


    Chord::IDMap ret = b->n;
      
    assert (ret.ip != me.ip);
    assert (ConsistentHash::betweenrightincl(me.id, key, ret.id));
      
    return ret;
      
  } else if (_style == TOE_NOTGREEDY_LAT) {
    int worst_lat = -1;
    Chord::IDMap b;
    //check the toes first
    for (uint i = 0; i < _toes.size (); i++) 
      {
	if (ConsistentHash::betweenrightincl(me.id, key, _toes[i].id)) {
	  int lat = t->latency (me.ip, _toes[i].ip);
	  if (lat > worst_lat) 
	    {
	      b = _toes[i];
	      worst_lat = lat;
	    }
	}
      }
    
    if (worst_lat > 0) {
      return b;
    } else
      //toes suck. just take something
      return LocTable::next_hop (key, done);

      
  } else if (_style == TOE_GREEDY_ID) {
    return LocTable::next_hop (key, done);
  } else if (_style == TOE_RTM) {

    ConsistentHash::CHID mindist = 0;
    bool found = false;
    Chord::IDMap b;
    //check the toes first
    for (uint i = 0; i < _toes.size (); i++) 
      {
	if (ConsistentHash::betweenrightincl(me.id, key, _toes[i].id)) {
	  ConsistentHash::CHID dist = ConsistentHash::distance (_toes[i].id, key);
	  assert (dist > 0);
	  if ((!found || dist < mindist)) 
	    {
	      found = true;
	      b = _toes[i];
	      mindist =  dist;
	    }
	}
      }
    
    Chord::IDMap ret;
    if (found) {
      ret = b;
    } else {
      //toes suck. just take something
      ret = LocTable::next_hop (key, done);
    }

    assert (ret.ip < 900);
    return ret;
  } else {
    assert (0);
    Chord::IDMap Ireturn;
    return Ireturn;
  }

}

ChordToe::ChordToe (Node *n, Args &a) : 
    ChordFingerPNS (n, a, new LocTableToe ()),  _numtoes (16)

{
  _lookup_style = a.nget<uint>("lookup_style",0,10);
  (dynamic_cast <LocTableToe *>(loctable))->set_style (_lookup_style);
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
  //  cerr << "considering toes: ";
  for (i = _toes.begin (); i != _toes.end (); ++i)
    {
      IDMap cur = *i;
      int lat = t->latency (me.ip, cur.ip);
      //    cerr << lat << " ";
      if (lat > worstlat) 
	{
	  worstlat = lat;
	  worst = i;
	}
    }
  
  // cerr << "; evicted " << worstlat << "\n";
  _toes.erase (worst);
  return worstlat;
}

void
ChordToe::init_state (vector<IDMap> ids) 
{
  loctable->set_evict (false);

  ChordFingerPNS::init_state (ids);

  int worst_lat = 1000000; //timeout is a good max

  Topology *t = Network::Instance()->gettopology ();

  //search for the best toes 
  for (uint i = 0; i < ids.size(); i++)
    {
      int lat = t->latency (me.ip, ids[i].ip);
      if ((lat < worst_lat || _toes.size () < _numtoes)) 
	worst_lat = add_toe (ids[i]);
    }

  //add the toes
  for (uint i = 0; i < _toes.size (); i++) {
    loctable->add_node (_toes[i]);
  }

  (dynamic_cast <LocTableToe *>(loctable))->set_toes (_toes);

}


