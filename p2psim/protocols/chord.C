#include "observers/chordobserver.h"
#include <stdio.h>
extern bool vis;
bool static_sim;

#define CHORD_DEBUG
Chord::Chord(Node *n, Args& a, LocTable *l)
  : DHTProtocol(n), _isstable (false)
{

  if(a.find("static_sim") != a.end())
    static_sim = true;

  //whether Chord uses vivaldi
  _vivaldi_dim = a.nget<uint>("vivaldidim", 0, 10);

  //location table timeout values
  _timeout = a.nget<uint>("timeout", 0, 10);

  //stabilization timer
  _stabtimer = a.nget<uint>("stabtimer", 10000, 10);

  //successors
  _nsucc = a.nget<uint>("successors", 1, 10);

  //fragments
  _frag = a.nget<uint>("m",1,10);

  //how many successors are fragments on?
  _allfrag = a.nget<uint>("allfrag",1,10);
  assert(_allfrag <= _nsucc);

  _asap = a.nget<uint>("asap",_frag,10);

  _vivaldi = NULL;

  assert(_frag <= _nsucc);

  me.ip = n->ip();
  me.id = ConsistentHash::ip2chid(me.ip); 
  me.choices = 0;

  if (l) 
    loctable = l;
  else
    loctable = New LocTable(_timeout);

  loctable->init (me);

  //pin down nsucc for successor list
  loctable->pin(me.id + 1, _nsucc, 0);
  //pin down 1 predecessor
  loctable->pin(me.id - 1, 0, 1);

  if (vis) {
    printf ("vis %llu node %16qx\n", now (), me.id);
  }
  _inited = false;

  stat.clear();
  _stab_running = false;
}

void
Chord::record_stat(uint type)
{
  while (stat.size() <= type) {
    stat.push_back(0);
  }
  assert(stat.size() > type);
  stat[type]++;
}

Chord::~Chord()
{
  printf("Chord done (%u,%qx)", me.ip, me.id);
  for (uint i = 0; i < stat.size(); i++) {
    printf(" %u", stat[i]);
  }
  printf("\n");

//  delete loctable; this gives me segfaults
  //delete loctable;
}

char *
Chord::ts()
{
  static char buf[50];
  sprintf(buf, "%llu %s(%u,%qx)", now(), proto_name().c_str(), me.ip, me.id);
  return buf;
}

void
Chord::check_static_init()
{
  if (!_inited) {
    assert(static_sim);
    _inited = true;
    this->init_state(ChordObserver::Instance(NULL)->get_sorted_nodes(0));
  }
}

void
Chord::lookup(Args *args) 
{
  check_static_init();

  CHID k = args->nget<CHID>("key");
  assert(k);

  uint recurs = args->nget<int>("recurs");

  Time begin = now();
  vector<IDMap> v;
  uint recurs_int = 0; //XXX: recurs has a different way of calculating lookup interval
  if (recurs) {
    v = find_successors_recurs(k, _frag, true, &recurs_int);
  } else {
    v = find_successors(k, _frag, true); 
  }

  if (v.size() == 0) {
    assert(!static_sim); 
    if (node()->alive()) 
      printf("%s lookup failed %16qx interval %llu\n", ts(),k,
	  now() - begin);
    return;
  } else { 
    uint vsz = v.size();
    assert((!static_sim) || (vsz >= _frag && vsz <= _allfrag));
    if (vsz < _frag) {
      printf("%s lookup insufficient key %16qx %d succs 
	  returned, %d requested\n",ts(), k, vsz, _frag);
      return; 
    }

    Topology *t = Network::Instance()->gettopology();
    vector<Time> lat;

    //calculate what is the random fetch time
    lat.clear();
    for (uint i = 0; i < vsz; i++) {
      //ignore bad data points from topology
      if (t->latency(me.ip,v[i].ip) < 1000000) { 
	lat.push_back(2*t->latency(me.ip,v[i].ip));
      }
      if (lat.size() >= _frag) break;
    }
    sort(lat.begin(), lat.end());
    if (lat.size() < _frag) return;

    Time rand_fetch_lat = lat[_frag-1];

    lat.clear();
    for (uint i = 0; i < vsz; i++) {
      lat.push_back(2 * t->latency(me.ip, v[i].ip));
    } 
    sort(lat.begin(), lat.end());
    Time fetch_lat = lat[_frag - 1];
 
    vector<IDMap> ids = ChordObserver::Instance(NULL)->get_sorted_nodes(0);
    IDMap tmp;
    tmp.id = k;
    uint pos = upper_bound(ids.begin(), ids.end(), tmp, 
			  Chord::IDMap::cmp) - ids.begin();
    while (1) {
      if (pos >= ids.size()) 
	pos = 0;

      Node *n = Network::Instance()->getnode(ids[pos].ip);
      if (n->alive()) 
	break;
      pos++;
    }

    for (uint i = 0; i < _allfrag; i++) {
      if (i < vsz && ids[pos].ip != v[i].ip) {
	printf("%s lookup incorrect key %16qx, %d succ should be (%u,%qx) 
		instead of (%u, %qx)\n", ts(), k, i, ids[pos].ip, 
		ids[pos].id, v[i].ip, v[i].id);
	return;
      }else if (i >= vsz) {
	lat.push_back(2*t->latency(me.ip,ids[pos].ip));
      }
      pos = (pos +1) % ids.size();
    }
    sort(lat.begin(), lat.end());

    uint interval = now() - begin;
    printf("%s asap %d recurs %d lookup %d succeeded %16qx %d succs interval %u %u %u %u %u\n", 
	ts(), _asap, recurs, _frag, k, vsz, 
	(unsigned) lat[_frag-1], (unsigned) rand_fetch_lat, (unsigned) fetch_lat, interval , recurs>0?recurs_int:interval);
  }

}

void
Chord::find_successors_handler(find_successors_args *args, 
			       find_successors_ret *ret)
{
  check_static_init();
  ret->v = find_successors(args->key, args->m, false);
}

// Returns at least m successors of key.
// This is the lookup() code in Figure 3 of the SOSP03 submission.
// A local call, use find_successors_handler for an RPC.
// Not recursive.
vector<Chord::IDMap>
Chord::find_successors(CHID key, uint m, bool is_lookup)
{
  assert(m <= _nsucc);

  int count = 0;

  vector<IDMap> route;

  if (vis && is_lookup) 
    printf ("vis %llu search %16qx %16qx\n", now(), me.id, key);

  next_args na;
  next_ret nr;

  na.key = key;
  na.m = m;

  IDMap nprime = me;

  route.clear();
  route.push_back(me);


  uint timeout = 0;

  while(1){
    assert(count++ < 500);
    if (vis && is_lookup) 
      printf ("vis %llu step %16qx %16qx\n", now(), me.id, nprime.id);

    
    bool r;

    record_stat(is_lookup?1:0);
    if (_vivaldi) {
      Chord *target = dynamic_cast<Chord *>(getpeer(nprime.ip));
      r = _vivaldi->doRPC(nprime.ip, target, &Chord::next_handler, &na, &nr);
    } else
      r = doRPC(nprime.ip, &Chord::next_handler, &na, &nr);

    if(r && nr.done){
      /*
      route.push_back(nr.v[0]);

      actually talk to the successor
      record_stat(is_lookup?1:0);
      nprime = nr.v[0];
      if (_vivaldi) {
	Chord *target = dynamic_cast<Chord *>(getpeer(nr.v[0].ip));
	r = _vivaldi->doRPC(nprime.ip, target, &Chord::null_handler, 
	    (void *)NULL, (void *)NULL);
      } else
	r = doRPC(nprime.ip, &Chord::null_handler, (void *)NULL, (void *)NULL);

      if (r) break;
      */
      break;
    } 
    
    if (r) {

      if (nr.next.ip == 0) {
	printf("%s ugly lookup failure has happened for %16qx\n",ts(),key);
	nr.v.clear();
	break;
      }

      route.push_back(nr.next);
      assert(route.size() < 20);
      nprime = nr.next;

      if (vis && is_lookup) 
	printf ("vis %llu step %16qx %16qx\n", now(), me.id, nr.next.id);

    } else {
      if (!node()->alive()) {
	printf ("%s initiator crashed in find_successor %qx\n", ts(), key);
	nr.v.clear();
	break;
      }
      timeout++;
      assert(route.size() >=2 && route.size() < 20);
      if ((route.size () > 1) && (node()->alive())) {
	route.pop_back (); 
	alert_args aa;
	alert_ret ar;
	aa.n = nprime;
	nprime = route.back ();
	record_stat(is_lookup?3:2);
	doRPC(nprime.ip, &Chord::alert_handler, &aa, &ar);
      } else {
	nr.v.clear ();
	break; 
      }
    }
  }

  if (is_lookup) {
    printf ("%s find_successor for key %qx ", ts(), key);
    if (nr.v.size () > 0) {
      printf ("is (%u, %qx) hops %d timeout %d\n", nr.v[0].ip, 
	  nr.v[0].id, route.size(), timeout);
    }
  }

#ifdef CHORD_DEBUG
  Topology *t = Network::Instance()->gettopology();
  if (nr.v.size() > 0) {
    printf("%s find_successors %qx route: ", ts(), key);
    for (uint i = 0; i < route.size(); i++) {
      printf("(%u, %qx, %d, %d) ", route[i].ip, route[i].id, 2 * (unsigned) t->latency(me.ip,route[i].ip), route[i].choices);
    }
    printf("\n");
  }
#endif
  return nr.v;
}

vector<Chord::IDMap>
Chord::find_successors_recurs(CHID key, uint m, bool is_lookup, uint *recurs_int)
{
  next_recurs_args fa;
  next_recurs_ret fr;
  fa.path.clear();

  lookup_path tmp;
  tmp.n = me;
  tmp.tout = 0;
  fa.path.push_back(tmp);

  fa.key = key;
  fa.m = m;
  fa.is_lookup = is_lookup;
  fr.v.clear();

  if (vis && is_lookup) 
    printf ("vis %llu search %16qx %16qx\n", now(), me.id, key);

#ifdef CHORD_DEBUG
  Time before = now();
#endif

  record_stat(fa.is_lookup?1:0);

  doRPC(me.ip, &Chord::next_recurs_handler, &fa, &fr);

  Topology *t = Network::Instance()->gettopology();
#ifdef CHORD_DEBUG
  printf("%s find_successors_recurs %qx route ",ts(),key);
#endif
  uint psz = fr.path.size();
  assert(psz > 0 && (!fr.path[psz-1].tout) && (!fr.path[0].tout));
  uint total_lat = 0;
  uint recurs_lat = 0;
  IDMap nn, np;
  for (uint i = 0; i < fr.path.size(); i++) {
    nn = fr.path[i].n;
    if (i == 0) {
      np = fr.path[psz-1].n;
    }
#ifdef CHORD_DEBUG
    printf("(%u,%qx,%u,%u) ", nn.ip, nn.id, (unsigned) t->latency(np.ip, nn.ip), nn.choices);
#endif
    recurs_lat += t->latency(np.ip, nn.ip);
    if (i > 0) {
      total_lat += 2 * t->latency(np.ip, nn.ip);
    }
    if (!fr.path[i].tout) {
      np = nn;
    }
  }
  printf("\n");
  uint interval = now() - before;
  assert(interval == total_lat);

  //XXX recursive lookup has a different way of calculating lookup latency
  if (recurs_int) {
    *recurs_int = recurs_lat;
  }

  if (is_lookup) {
    printf("%s lookup key %qx,%d, hops %d\n", ts(), key, m, psz);
  }

  return fr.v;
}

//XXX: in a dynamic environment, the current implementation has a 
//wierd mechanism for implementing timeout.
//it sends an empty reply to the sender to indicate that 
//some node currently holding the packet has died
//in a real implemenation, the sender should implement some kind of 
//timeout mechanism.
void
Chord::next_recurs_handler(next_recurs_args *args, next_recurs_ret *ret)
{
  check_static_init();

  vector<IDMap> succs = loctable->succs(me.id+1, _nsucc);

  assert((!static_sim) || succs.size() == _nsucc);

  if (succs.size() < 1) {
    //lookup failed
    //XXX do i need to backtrack?
    ret->v.clear();
    ret->path = args->path;
    return;
  }

#ifdef STOP_EARLY
  ret->v.clear();
  for (uint i = 0; i < succs.size(); i++) {
    if (ConsistentHash::betweenrightincl(me.id, succs[i].id, args->key)) {
      ret->v.push_back(succs[i]);
      if (ret->v.size() == _allfrag) break;
    }
  }
  if (ret->v.size() >= args->m) {
    printf("yeow, %s stopped early with %d succs\n", ts(), ret->v.size());
    ret->path = args->path;
    if (!node->alive()) { 
      ret->v.clear();
    }
    return;
  }
#endif

  if (ConsistentHash::betweenrightincl(me.id, succs[0].id, args->key) 
      || (succs.size() < args->m)) { //this means there's < m nodes in the system 
    printf("yeow, %s come to predecessor %d succs\n", ts(), ret->v.size());
    ret->v.clear();
    if (node()->alive()) {
      for (uint i = 0; i < _allfrag; i++) {
	ret->v.push_back(succs[i]);
      }
    }
    ret->path = args->path;
  }else {
    assert(args->path.size() < 100);

    //XXX i never check if succ is dead or not
    bool r = false;
    lookup_path tmp;
    while (!r) {
      bool done;
      IDMap next = loctable->next_hop(args->key, &done,1,1);
      assert(!done);
      assert(next.choices > 0);

      tmp.n = next;
      tmp.tout = 0;
      args->path.push_back(tmp);

      record_stat(args->is_lookup?1:0);
      if (_vivaldi) {
	Chord *target = dynamic_cast<Chord *>(getpeer(next.ip));
	r = _vivaldi->doRPC(next.ip, target, &Chord::next_recurs_handler, args, ret);
      } else
	r = doRPC(next.ip, &Chord::next_recurs_handler, args, ret);

      assert(r);
      if ((!r) && (node()->alive())) {
	printf ("%16qx rpc to %16qx failed %llu\n", me.id, next.id, now ());
	if (vis) 
	  printf ("vis %llu delete %16qx %16qx\n", now (), me.id, next.id);
	args->path[args->path.size()-1].tout = 1;
	loctable->del_node(next);
      }
    }
  }
}

void
Chord::null_handler (void *args, void *ret) 
{
  return;
}

// Always attempt to go to the predecessor for the key regardless of m
void
Chord::next_handler(next_args *args, next_ret *ret)
{
  check_static_init();

  vector<IDMap> succs = loctable->succs(me.id+1, _nsucc);
  assert((!static_sim) || succs.size() >= _allfrag);

  if (succs.size() < 1) {
    ret->v.clear();
    ret->done = false;
    IDMap tmp;
    tmp.ip = 0;
    ret->next = tmp;
  } else if (ConsistentHash::betweenrightincl(me.id, succs[0].id, args->key)) { 
    //XXX: need to take care of < m nodes situation in future
    ret->v.clear();
    for (uint i = 0; i < _allfrag; i++) {
      ret->v.push_back(succs[i]);
    }
    ret->done = true;
  } else {
    ret->done = false;
    bool done;
    IDMap next = loctable->next_hop(args->key, &done);
    if (!done) {
      ret->next = next;
      assert(ret->next.ip != me.ip);
    } else {
      ret->done = true;
      ret->v.clear();
      ret->v.push_back(next);
    }
 }

}

// External event that tells a node to contact the well-known node
// and try to join.
void
Chord::join(Args *args)
{
  assert(!static_sim);

  if (vis) {
    printf("vis %llu join %16qx\n", now (), me.id);
  }

  _inited = true;

  if (_vivaldi_dim > 0) {
    _vivaldi = New Vivaldi10(node(), _vivaldi_dim, 0.05, 1); 
  }

  IDMap wkn;
  wkn.ip = args->nget<IPAddress>("wellknown");
  assert (wkn.ip);
  wkn.id = ConsistentHash::ip2chid(wkn.ip);
  loctable->add_node (wkn);

  Time before = now();
  find_successors_args fa;
  find_successors_ret fr;
  fa.key = me.id + 1;
  fa.m = 1;

  record_stat();
  bool ok = doRPC(wkn.ip, &Chord::find_successors_handler, &fa, &fr);

  assert (ok);
  assert(fr.v.size() > 0);
  Time after = now();
  printf("%s join2 %16qx, elapsed %llu\n",
         ts(), fr.v[0].id,
         after - before);
  loctable->add_node(fr.v[0]);

  if (!_stab_running) {
    _stab_running = true;
    reschedule_stabilizer(NULL);
  }
}


void
Chord::reschedule_stabilizer(void *x)
{
  assert(0);
  assert(!static_sim);
  if (!node()->alive()) {
    _stab_running = false;
    return;
  }

  Time t = now();
  stabilize();

  if ( t + _stabtimer >= now()) { //stabilizating has run past _stabtime seconds
    reschedule_stabilizer(NULL);
  }else{
    delaycb(now() - t - _stabtimer, &Chord::reschedule_stabilizer, (void *) 0);
  }
}

// Which paper is this code from? -- PODC 
// stabilization is not run in one non-interruptable piece
// after each possible yield point, check if the node is dead.
void
Chord::stabilize()
{

  //ping my predecessor
  IDMap pred = loctable->pred(me.id-1);
  assert(pred.ip);
  bool ok;
  get_successor_list_args gsa;
  get_successor_list_ret gsr;

  gsa.m = 1;
  record_stat();
  if (_vivaldi) {
      Chord *target = dynamic_cast<Chord *>(getpeer(pred.ip));
      ok = _vivaldi->doRPC(pred.ip, target, 
	  &Chord::get_successor_list_handler, &gsa, &gsr);
  } else 
      ok = doRPC(pred.ip, &Chord::get_successor_list_handler, &gsa, &gsr);

  if (!node()->alive()) return;

  if (ok) {
    loctable->add_node(pred); //refresh timestamp
    if (gsr.v.size() > 0)
      loctable->add_node(gsr.v[0]);
  } else
    loctable->del_node(pred); 


  IDMap succ1 = loctable->succ(me.id+1);

  //ping my successor and get my successor's predecessor
  IDMap pred1 = fix_successor ();

  if (!node()->alive()) return;

  if (pred1.ip != me.ip && ConsistentHash::between(pred1.id, succ1.id, me.id)) {
    //my successor's predecessor is behind me
    //notify my succ of his predecessor change
    notify_args na;
    notify_ret nr;
    na.me = me;

    record_stat();
    bool ok;
    if (_vivaldi) {
      Chord *target = dynamic_cast<Chord *>(getpeer(succ1.ip));
      ok = _vivaldi->doRPC(succ1.ip, target, &Chord::notify_handler, &na, &nr);
    } else
      ok = doRPC(succ1.ip, &Chord::notify_handler, &na, &nr);
    
    if (!ok) {
      loctable->del_node(succ1);
    } 
  }

  if (!node()->alive()) return;

  //get succ list from my successor
  if (_nsucc > 1) fix_successor_list();
}

bool
Chord::stabilized(vector<CHID> lid)
{
  vector<CHID>::iterator iter;
  iter = find(lid.begin(), lid.end(), me.id);
  assert(iter != lid.end());

  vector<IDMap> succs = loctable->succs(me.id+1, _nsucc);

#if 0
  printf ("stable? successor list %u,%16qx at %lu\n", me.ip, me.id, now ());
  for (unsigned int i = 0; i < succs.size (); i++) {
    printf (" successor %d: %u, %16qx\n", i, succs[i].ip, succs[i].id);
  }
#endif

  if (succs.size() != _nsucc) 
    return false;

  for (unsigned int i = 1; i <= _nsucc; i++) {
    iter++;
    if (iter == lid.end()) iter = lid.begin();
    if (succs[i-1].id != *iter) {
      printf("%s not stabilized, %5d succ should be %16qx instead of (%u, %16qx)\n", 
	  ts(), i-1, *iter, succs[i-1].ip,  succs[i-1].id);
      return false;
    }
  }

  return true;
}

void
Chord::init_state(vector<IDMap> ids)
{
  loctable->add_sortednodes(ids);
  IDMap succ1 = loctable->succ(me.id+1);
  printf("%s inited %d %d succ is %u,%qx\n", ts(), ids.size(), 
      loctable->size(), succ1.ip, succ1.id);
  _inited = true;
}

Chord::IDMap
Chord::fix_successor()
{
  // Time t = now();
  IDMap succ1 = loctable->succ(me.id+1);
  if (succ1.ip == me.ip) return me;


  get_predecessor_args gpa;
  get_predecessor_ret gpr;
  bool ok;

  record_stat();
  if (_vivaldi) {
    Chord *target = dynamic_cast<Chord *>(getpeer(succ1.ip));
    ok = _vivaldi->doRPC(succ1.ip, target, &Chord::get_predecessor_handler, 
	&gpa, &gpr);
  } else
    ok = doRPC(succ1.ip, &Chord::get_predecessor_handler, &gpa, &gpr);

  if (!ok) {
#ifdef CHORD_DEBUG
    printf("%s fix_successor old succcessor (%u,%qx) died\n", 
	ts(), succ1.ip, succ1.id);
#endif
    loctable->del_node(succ1);
    return me;
  }else{
    loctable->add_node(succ1); //refresh timestamp
  }

#ifdef CHORD_DEBUG
  printf("%s fix_successor successor (%u,%qx)'s predecessor is (%u, %qx)\n", 
      ts(), succ1.ip, succ1.id, gpr.n.ip, gpr.n.id);
#endif

  if (gpr.n.ip && gpr.n.ip!= me.id) loctable->add_node(gpr.n);
  return gpr.n;
}

void
Chord::get_successor_list_handler(get_successor_list_args *args, 
				  get_successor_list_ret *ret)
{
  check_static_init();
  ret->v = loctable->succs(me.id+1, args->m);
}


void
Chord::fix_successor_list()
{
  IDMap succ = loctable->succ(me.id+1);

  get_successor_list_args gsa;
  get_successor_list_ret gsr;
  bool ok;

  gsa.m = _nsucc;

  record_stat();
  if (_vivaldi) {
    Chord *target = dynamic_cast<Chord *>(getpeer(succ.ip));
    ok = _vivaldi->doRPC(succ.ip, target, &Chord::get_successor_list_handler, 
	&gsa, &gsr);
  } else
    ok = doRPC(succ.ip, &Chord::get_successor_list_handler, &gsa, &gsr);

#ifdef CHORD_DEBUG
  vector<IDMap> v = loctable->succs(me.id+1,_nsucc);
  printf("%s fix_successor_list: ",ts());
  for (uint i = 0; i < v.size(); i++) {
    printf("%u,%qx ",v[i].ip, v[i].id);
  }
  printf("\n");
#endif

  if ((!ok) && (node()->alive())) {
    loctable->del_node(succ);
    return;
  }

  for (unsigned int i = 0; i < (gsr.v).size(); i++) {
    loctable->add_node(gsr.v[i]);
  }

  if (vis) {
    bool change = false;

    vector<IDMap> scs = loctable->succs(me.id + 1, _nsucc);
    for (uint i = 0; i < scs.size (); i++) {
      if ((i >= lastscs.size ()) || lastscs[i].id != scs[i].id) {
	change = true;
      }
    }
    if (change) {
      printf ( "vis %llu succlist %16qx", now (), me.id);
      for (uint i = 0; i < scs.size (); i++) {
	printf ( " %16qx", scs[i].id);
      }
      printf ( "\n");
    }
    lastscs = scs;
  }
}


void
Chord::notify_handler(notify_args *args, notify_ret *ret)
{
  assert(!static_sim);
  IDMap p1 = loctable->pred();
  loctable->add_node(args->me);
}


void
Chord::alert_handler(alert_args *args, alert_ret *ret)
{
  assert(!static_sim);
  if (vis)
    printf ("vis %llu delete %16qx %16qx\n", now (), me.id, args->n.id);
  loctable->del_node(args->n);
}

void
Chord::get_predecessor_handler(get_predecessor_args *args,
                               get_predecessor_ret *ret)
{
  assert(!static_sim);
  ret->n = loctable->pred();
}

void
Chord::dump()
{
  _isstable = true;

  printf("myID is %16qx %5u\n", me.id, me.ip);
  printf("===== %16qx =====\n", me.id);
  printf ("ring size %d\n", loctable->size ());

  vector<IDMap> v = loctable->succs(me.id+1, _nsucc);
  for (unsigned int i = 0; i < v.size(); i++) {
    printf("%16qx: succ %5d : %16qx\n", me.id, i, v[i].id);
  }
  IDMap p = loctable->pred();
  printf("pred is %5u,%16qx\n", p.ip, p.id);
}

void
Chord::leave(Args *args)
{
  assert(!static_sim);
  crash (args);
  loctable->del_all();
  loctable->init(me);
}

void
Chord::crash(Args *args)
{
  assert(!static_sim);
  if (vis)
    printf ("vis %llu crash %16qx\n", now (), me.id);

  node()->crash ();
  loctable->del_all();
  loctable->init(me);
  printf("%s crashed! loctable sz %d\n",ts(), loctable->size());

}


/*************** LocTable ***********************/

LocTable::LocTable(uint timeout) 
{
  _timeout = timeout;
  _evict = false;
} 

void LocTable::init (Chord::IDMap m)
{
  me = m;
  pin(me.id, 1, 0);
  idmapwrap *elm = New idmapwrap(me, now());

  bool ok = ring.insert(elm);
  assert(ok);
}

void
LocTable::del_all()
{
  assert (ring.repok ());
  idmapwrap *next;
  idmapwrap *cur;
  for (cur = ring.first(); cur; cur = next) {
    next = ring.next(cur);
    ring.remove(cur->id);
    bzero(cur, sizeof(*cur));
    delete cur;
  }
  assert (ring.repok ());
}

LocTable::~LocTable()
{
  del_all();
}

//get the succ node including or after this id 
Chord::IDMap
LocTable::succ(ConsistentHash::CHID id)
{
  if (size() == 1) {
    return me;
  }
  vector<Chord::IDMap> v = succs(id, 1);
  assert(v.size() > 0);
  return v[0];
}

/* returns m successors including or after the number id*/
vector<Chord::IDMap>
LocTable::succs(ConsistentHash::CHID id, unsigned int m)
{
  vector<Chord::IDMap> v;
  v.clear();

  assert (ring.repok ());
  Time t = now();

  if (m <= 0) return v;

  idmapwrap *ptr = ring.closestsucc(id);
  assert(ptr);
  idmapwrap *ptrnext;

  uint j = 0;
  while (j < m) {
    ptrnext = ring.next(ptr);
    if (!ptrnext) ptrnext = ring.first();

    if ((!_timeout)||(t - ptr->timestamp) < _timeout) {
      v.push_back(ptr->n);
      j++;
      if (j >= ring.size()) return v;
    }else{
      ring.remove(ptr->id);
      //printf("%u,%qx del %p\n", me.ip, me.id, ptr);
      bzero(ptr, sizeof(*ptr));
      delete ptr;
      if (ring.size() <= 1) {
	return v;
      }
      //assert(ring.size() >= 2);
    }
    ptr = ptrnext;
  }
  return v;
}

Chord::IDMap
LocTable::pred()
{
  return pred(me.id -1);
}

Chord::IDMap
LocTable::pred(Chord::CHID id) 
{
  assert (ring.repok ());
  idmapwrap *elm = ring.closestpred(id);
  assert(elm);
  idmapwrap *elmprev;

  Time t = now();
  uint deleted = 0;
  uint rsz = ring.size();
  while (1) {
    elmprev = ring.prev(elm);
    if (!elmprev) elmprev = ring.last();

    if ((!_timeout) || (elm->id == me.id)
	|| (t - elm->timestamp < _timeout)) { //never delete myself
      break;
    }else {
      ring.remove(elm->id);
      //printf("%u,%qx del %p\n", me.ip, me.id, elm);
      bzero(elm, sizeof(*elm));
      delete elm;
      deleted++;
      elm = elmprev;
    }
    assert((rsz - deleted >= 1));
  }
  assert(elm->n.id == me.id || 
      ConsistentHash::betweenrightincl(me.id, id, elm->n.id));
  return elm->n;
}

void
LocTable::print ()
{
  idmapwrap *m = ring.search(me.id);
  assert(m);
  printf ("ring:\n");
  idmapwrap *i = m;
  do {
    printf ("  %5u,%16qx\n", i->n.ip, i->n.id);
    i = ring.next(i);
    if (!i) i = ring.first();
  }while (i != m);
}

/* adds a list of nodes, sorted by increasing CHIDs
 * this function will avoid regular eviction calls by adding only pinned nodes
 */
void
LocTable::add_sortednodes(vector<Chord::IDMap> l)
{
  uint sz = pinlist.size();
  int lsz = l.size();
  Chord::IDMap tmppin;
  tmppin.ip = 0;
  int pos;
  int ptr;
  Chord::IDMap n;
  pin_entry p;

  for (uint i = 0; i < sz; i++) {
    p = pinlist[i];
    tmppin.id = pinlist[i].id;
    pos = upper_bound(l.begin(),l.end(),tmppin,Chord::IDMap::cmp) - l.begin();
    if (pos >= lsz) pos = 0;
    ptr = pos;
    if (pos < lsz && (l[pos].id != tmppin.id)) {
      ptr--;
    }
    for (uint k = 0; k < pinlist[i].pin_pred; k++) {
      if (ptr< 0) ptr= (lsz-1);
      n = l[ptr];
      add_node(l[ptr]);
      ptr--;
    }
    ptr = pos; 
    for (uint k = 0; k < pinlist[i].pin_succ; k++) {
      if (ptr >= lsz) ptr= 0;
      n = l[ptr];
      add_node(l[ptr]);
      ptr++;
    }
  }
}

void
LocTable::add_node(Chord::IDMap n)
{
//  assert(n.choices > 0);
  Chord::IDMap succ1; 
  Chord::IDMap pred1; 

  assert(n.ip > 0 && n.ip < 32000);
  if (vis) {
    succ1 = succ(me.id+1);
    pred1 = pred();
  }

  idmapwrap *elm = ring.search(n.id);
  if (elm) {
    elm->timestamp = now();
    assert (ring.repok ());
    return;
  } else {
    elm = New idmapwrap(n,now());
    //printf("%u,%qx New %p\n", me.ip, me.id, elm);
    assert(elm && elm->n.ip);
    if (ring.insert(elm)) {
    }else{
      assert(0);
    }
  }

  assert (ring.repok ());

  if (_evict && ring.size() > _max) {
    evict();
  }

  if (vis) {

    Chord::IDMap succ2 = succ(me.id + 1);
    Chord::IDMap pred2 = pred ();

    if(succ1.id != succ2.id) {
      printf("vis %llu succ %16qx %16qx\n", now (), me.id, succ2.id);
    }

    if(pred1.id != pred2.id) {
      printf("vis %llu pred %16qx %16qx\n", now (), me.id, pred2.id);
    }
  }
}

void
LocTable::del_node(Chord::IDMap n)
{
  assert(n.ip != me.ip);
  idmapwrap *elm = ring.search(n.id);
  if (!elm) return;

  elm = ring.remove(n.id);
  //printf("%u,%qx del %p\n", me.ip, me.id, elm);
  bzero(elm,sizeof(*elm));
  delete elm;
  assert (ring.repok ());
}

void
LocTable::clear_pins()
{
  uint sz = pinlist.size();
  for (uint i = 0; i < (sz -1); i++) {
    pinlist.pop_back();
  }
}

// pin maintains sorted list of id's that must be pinned
void
LocTable::pin(Chord::CHID x, uint pin_succ, uint pin_pred)
{
  pin_entry pe;

  pe.id = x;
  pe.pin_succ = pin_succ;
  pe.pin_pred = pin_pred;

  _max += (pin_succ + pin_pred);

  if (pinlist.size () == 0) {
    pinlist.push_back(pe);
    return;
  }

  uint i = 0;
  for (; i < pinlist.size(); i++) {
    if (ConsistentHash::betweenrightincl(pinlist[0].id, pinlist[i].id, x)) {
      break;
    }
  }

  if (i < pinlist.size() && pinlist[i].id == x) {
    if (pin_pred > pinlist[i].pin_pred) 
      pinlist[i].pin_pred = pin_pred;
    if (pin_succ > pinlist[i].pin_succ)
      pinlist[i].pin_succ = pin_succ;
  } else {
    if (pinlist.size () == i) {
      pinlist.push_back(pe);
    } else {
      assert(pinlist[0].id != pe.id);
      pinlist.insert(pinlist.begin() + i, pe);
    }
  }
  assert(pinlist.size() <= _max);
}

uint
LocTable::size()
{
  return ring.size();
}

void
LocTable::evict() // all unnecessary(unpinned) nodes 
{
  assert(0);
  assert(pinlist.size() <= _max);
  assert(pinlist.size() > 0);


  idmapwrap *ptr;
  idmapwrap *elm = ring.first();
  while (elm) {
    elm->pinned = false;
    elm = ring.next(elm);
  }
  
  uint i = 0; // index into pinlist

  elm = ring.first();
  while (i < pinlist.size ()) {

    // find successor of pinlist[i]. 
    //XXX don't start at j, but where we left off
    while (elm && elm->id < pinlist[i].id) {
      elm = ring.next(elm);
    }
    if (elm && elm->id > pinlist[i].id ) {
      ptr = ring.prev(elm);
    }else {
      ptr = elm;
    }

    // pin the predecessors
    for (uint k = 0; k < pinlist[i].pin_pred; k++) {
      if (!ptr) ptr = ring.last();
      ptr->pinned = true;
      ptr = ring.prev(ptr);
    }

    ptr = elm;
    // pin the successors, starting with j
    for (uint k = 0; k < pinlist[i].pin_succ; k++) {
      if (!ptr) ptr = ring.first();
      ptr->pinned = true;
      ptr = ring.next(ptr);
    }
    i++;
  }
  // evict entries that are not pinned
  elm = ring.first();
  idmapwrap *next; 
  while (elm) {
    next = ring.next(elm);
    if (elm->pinned) {
      elm->pinned = false;
    }else{
      ring.remove(elm->id);
      delete elm;
      assert (ring.repok ());
    } 
    elm = next;
  }

  assert(elm == NULL);
}

Chord::IDMap
LocTable::next_hop(Chord::CHID key, bool *done, uint m, uint nsucc)
{
  if (done) *done = false; 
  return pred(key);
}

vector<Chord::IDMap>
LocTable::get_all()
{
  vector<Chord::IDMap> v;
  v.clear();

  idmapwrap *currp; 
  currp = ring.first();
  while (currp) {
    v.push_back(currp->n);
    currp = ring.next(currp);
  }
  return v;
}

