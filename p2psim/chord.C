#include "chord.h"
#include "node.h"
#include "packet.h"
#include "topology.h"
#include "network.h"
#include "vivaldi.h"
#include "chordobserver.h"
#include <stdio.h>
#include <iostream>
#include <algorithm>

using namespace std;
extern bool vis;
extern bool static_sim;

#undef CHORD_DEBUG
Chord::Chord(Node *n, Args& a, LocTable *l)
  : DHTProtocol(n), _isstable (false)
{

  //if Chord uses vivaldi
  _vivaldi_dim = a.nget<uint>("vivaldidim", 0, 10);

  //location table timeout values
  _timeout = a.nget<uint>("timeout", 0, 10);

  //stabilization timer
  _stabtimer = a.nget<uint>("stabtimer", 10000, 10);

  //successors
  _nsucc = a.nget<uint>("successors", 3, 10);

  _vivaldi = NULL;

  me.ip = n->ip();
  me.id = ConsistentHash::ip2chid(me.ip); 

  if (l) 
    loctable = l;
  else
    loctable = new LocTable(_timeout);

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
  delete loctable;

  for (uint i = 0; i < stat.size(); i++) {
    printf(" %u", stat[i]);
  }
  printf("\n");
}

char *
Chord::ts()
{
  static char buf[50];
  sprintf(buf, "%llu Chord(%5u,%16qx)", now(), me.ip, me.id);
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
  Topology *t = Network::Instance()->gettopology();
  IDMap result;
  if (recurs) {
    result = find_successors_recurs(k, false, true);
    uint lat = t->latency(me.ip, result.ip);
    printf("%s lookup succeeded %16qx (%u,%16qx) interval %u %llu\n", ts(),k, 
	result.ip, result.id, 2 * lat, now() - begin);
  } else {
    printf("%s lookup start\n",ts());
    vector<IDMap> v = find_successors(k, 1, true); 
    if (v.size() == 0) {
      if (node()->alive()) {
	printf("%s lookup failed %16qx interval %llu\n", ts(),k, 
	    now() - begin);
	return;
      }
    }else{
      result = v[0];
      uint lat = t->latency(me.ip, v[0].ip);
      printf("%s lookup succeeded %16qx (%u,%16qx) interval %u %llu\n", ts(),k, 
	  v[0].ip, v[0].id, 2 * lat, now() - begin);
    }
  }

#ifdef CHORD_DEBUG
  vector<IDMap> ids = ChordObserver::Instance(NULL)->get_sorted_nodes(0);
  IDMap tmp;
  tmp.id = k;
  uint pos = upper_bound(ids.begin(), ids.end(), tmp, Chord::IDMap::cmp) - ids.begin();
  while (1) {
    if (pos >= ids.size()) 
      pos = 0;

    Node *n = Network::Instance()->getnode(ids[pos].ip);
    if (n->alive()) 
      break;
    pos++;
  }
  if (ids[pos].ip == result.ip) {
    printf("%s lookup result correct %u,%16qx for key %16qx \n", ts(), result.ip, result.id, k);
  }else{
    printf("%s lookup result incorrect %u,%16qx for key %16qx, should be %u,%qx\n", ts(), result.ip, result.id, k, ids[pos].ip, ids[pos].id);
  }
#endif
}

void
Chord::find_successors_handler(find_successors_args *args, find_successors_ret *ret)
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
      route.push_back(nr.v[0]);
      //actually talk to the successor
      record_stat(is_lookup?1:0);
      if (_vivaldi) {
	Chord *target = dynamic_cast<Chord *>(getpeer(nr.v[0].ip));
	r = _vivaldi->doRPC(nr.v[0].ip, target, &Chord::null_handler, (void *)NULL, (void *)NULL);
      } else
	r = doRPC(nr.v[0].ip, &Chord::null_handler, (void *)NULL, (void *)NULL);

      if (vis && is_lookup) 
	printf ("vis %llu step %16qx %16qx\n", now(), me.id, nr.v[0].id);

      break;
    } else if (r) {
      route.push_back(nr.next);
      assert(route.size() < 15);
      nprime = nr.next;

    } else {
      if (!node()->alive()) {
	printf ("%s initiator crashed in find_successor %qx\n", ts(), key);
	nr.v.clear();
	return nr.v;
      }
      timeout++;
      assert(route.size() >=2 && route.size() < 15);
      if (route.size () > 1) {
	route.pop_back (); 
	alert_args aa;
	alert_ret ar;
	aa.n = nprime;
	nprime = route.back ();
	record_stat(is_lookup?1:0);
	doRPC(nprime.ip, &Chord::alert_handler, &aa, &ar);
      } else {
	nr.v.clear ();
	break; 
      }
    }
  }
  if (is_lookup) {
    printf ("find_successor for (id %qx, key %qx)", me.id, key);
    if (nr.v.size () == 0) {
      printf ("failed\n");
    } else {
      printf ("is (%u, %qx) hops %d timeout %d\n", nr.v[0].ip, nr.v[0].id, count+1, timeout);
    }
  }
  return nr.v;
}

Chord::IDMap 
Chord::find_successors_recurs(CHID key, bool intern, bool is_lookup)
{
  next_recurs_args fa;
  next_recurs_ret fr;
  fa.v.clear();
  fa.v.push_back(me);
  fa.key = key;
  fa.is_lookup = is_lookup;

  if (!intern) {
    printf("%s find_successors_recurs for key %qx\n", ts(), key);
  }

  record_stat(fa.is_lookup?1:0);
  doRPC(me.ip, &Chord::next_recurs_handler, &fa, &fr);

  IDMap succ = fr.v.back();

#ifdef CHORD_DEBUG
  Time before = now();
  Topology *t = Network::Instance()->gettopology();
  printf("%s find_successors_recurs %qx route ",ts(),key);
  uint total_lat = 0;
  for (uint i = 0; i < fr.v.size(); i++) {
    IDMap n = fr.v[i];
    printf("(%u,%qx,%u, %u) ", n.ip, n.id, total_lat, i < (fr.v.size()-1)? 2 * t->latency(fr.v[i].ip, fr.v[i+1].ip): 0);
    if (i < (fr.v.size() - 1)) {
      total_lat += 2 * t->latency(fr.v[i].ip,fr.v[i+1].ip);
    }
  }
  uint interval = now() - before;
  printf("\n");

  printf("%s lookup %16qx results (%u,%16qx) hops %d interval %u %u %u %u\n", ts(), key, succ.ip, succ.id, fr.v.size(), 2 * t->latency(me.ip, succ.ip), total_lat, interval, total_lat + t->latency(fr.v[fr.v.size()-3].ip, fr.v[fr.v.size()-1].ip));
  //assert(interval == total_lat);

#endif
  return succ;
}
void
Chord::next_recurs_handler(next_recurs_args *args, next_recurs_ret *ret)
{
  check_static_init();

  IDMap succ = loctable->succ(me.id+1);
  IDMap pred = loctable->pred();
  /*
  if (ConsistentHash::betweenrightincl(pred.id, me.id, args->key)) {
    ret->v = args->v;
  }else */if (ConsistentHash::betweenrightincl(me.id, succ.id, args->key) || succ.id == me.id) {
    printf("haha\n");
    ret->v = args->v;
    ret->v.push_back(succ);
  }else{

    bool r = false;
#ifdef CHORD_DEBUG
    uint x = find(args->v.begin(), args->v.end(), me) - args->v.begin();
    if (x != (args->v.size() - 1)) {
      printf("%s error! back to me!\n", ts());
      for (uint xx = 0; xx < args->v.size(); xx++) {
	printf("route %u,%qx\n", args->v[xx].ip, args->v[xx].id);
      }
      abort();
    }
#endif

    assert(args->v.size() < 100);

    //XXX i never check if succ is dead or not
    while (!r) {
      bool done;
      IDMap next = loctable->next_hop(args->key, &done);
      if (done) {
	ret->v = args->v;
	ret->v.push_back(next);
	return;
      }
      args->v.push_back(next);

      record_stat(args->is_lookup?1:0);
      if (_vivaldi) {
	Chord *target = dynamic_cast<Chord *>(getpeer(next.ip));
	r = _vivaldi->doRPC(next.ip, target, &Chord::next_recurs_handler, args, ret);
      } else
	r = doRPC(next.ip, &Chord::next_recurs_handler, args, ret);

      assert(r);
      if (!r) {
	printf ("%16qx rpc to %16qx failed %llu\n", me.id, next.id, now ());
	if (vis) 
	  printf ("vis %llu delete %16qx %16qx\n", now (), me.id, next.id);
	args->v.pop_back();
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
// From Figure 3 of SOSP03 submission.
// Returns either m successors of the given key,
// or the node to talk to next.
void
Chord::next_handler(next_args *args, next_ret *ret)
{
  check_static_init();

  IDMap succ = loctable->succ(me.id+1);
  if(ConsistentHash::betweenrightincl(me.id, succ.id, args->key) ||
     succ.id == me.id){
#ifdef CHORD_DEBUG
    printf("%s i think it is done %u,%qx (%qx, %qx, %qx)\n", ts(), succ.ip, succ.id, me.id, args->key, succ.id);
#endif
    Node *n = Network::Instance()->getnode(succ.ip);
    if (!n->alive())
      printf("really succ is dead %d\n", succ.ip);
    ret->done = true;
    ret->v.clear();
    ret->v.push_back(succ);
 } else {
    ret->done = false;
    // XXX for iterative, me should really be the requesting node
    bool done;
    IDMap next = loctable->next_hop(args->key,&done);
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
    _vivaldi = new Vivaldi10(node(), _vivaldi_dim, 0.05, 1); 
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
      ok = _vivaldi->doRPC(pred.ip, target, &Chord::get_successor_list_handler, &gsa, &gsr);
  } else 
      ok = doRPC(pred.ip, &Chord::get_successor_list_handler, &gsa, &gsr);

  if (ok) {
    loctable->add_node(pred); //refresh timestamp
    if (gsr.v.size() > 0)
      loctable->add_node(gsr.v[0]);
  } else
    loctable->del_node(pred); 


  IDMap succ1 = loctable->succ(me.id+1);

  //ping my successor and get my successor's predecessor
  IDMap pred1 = fix_successor ();

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
      printf("%s not stabilized, %5d succ should be %16qx instead of (%u, %16qx)\n", ts(), i-1, *iter, succs[i-1].ip,  succs[i-1].id);
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
  printf("Chord: %s inited %d %d succ is %u,%qx\n", ts(), ids.size(), loctable->size(), succ1.ip, succ1.id);
  _inited = true;
}

Chord::IDMap
Chord::fix_successor()
{
  // Time t = now();
  IDMap succ1 = loctable->succ(me.id+1);

  get_predecessor_args gpa;
  get_predecessor_ret gpr;
  bool ok;

  record_stat();
  if (_vivaldi) {
    Chord *target = dynamic_cast<Chord *>(getpeer(succ1.ip));
    ok = _vivaldi->doRPC(succ1.ip, target, &Chord::get_predecessor_handler, &gpa, &gpr);
  } else
    ok = doRPC(succ1.ip, &Chord::get_predecessor_handler, &gpa, &gpr);

  if (!ok) {
#ifdef CHORD_DEBUG
    printf("%s fix_successor old (%llu) succcessor (%u,%qx) died\n", ts(), t, succ1.ip, succ1.id);
#endif
    loctable->del_node(succ1);
    return me;
  }else{
    loctable->add_node(succ1); //refresh timestamp
  }

#ifdef CHORD_DEBUG
  printf("%s fix_successor (%llu) successor (%u,%qx)'s predecessor is (%u, %qx)\n", ts(), t,succ1.ip, succ1.id, gpr.n.ip, gpr.n.id);
#endif

  if (gpr.n.ip && gpr.n.ip!= me.id) loctable->add_node(gpr.n);
  return gpr.n;
}

void
Chord::get_successor_list_handler(get_successor_list_args *args, get_successor_list_ret *ret)
{
  assert(!static_sim);
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
    ok = _vivaldi->doRPC(succ.ip, target, &Chord::get_successor_list_handler, &gsa, &gsr);
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

  if (!ok) {
    loctable->del_node(succ);
    return;
  }

  for (unsigned int i = 0; i < (gsr.v).size(); i++) {
    loctable->add_node(gsr.v[i]);
  }

  // printf ("fix_successor_list: %u,%16qx at %lu succ %u,%16qx\n", me.ip, me.id, 
  //now(), succ.ip, succ.id);
  
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
}

void
Chord::crash(Args *args)
{
  assert(!static_sim);
  if (vis)
    printf ("vis %llu crash %16qx\n", now (), me.id);
#ifdef CHORD_DEBUG
  printf("%s crashed\n", ts());
#endif
  node()->crash ();
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
  idmapwrap *elm = new idmapwrap(me, now());

  ring.insert(elm);
}

LocTable::~LocTable()
{
  idmapwrap *next;
  idmapwrap *cur;
  for (cur = ring.first(); cur; cur = next) {
    next = ring.next(cur);
    delete cur;
  }
}
//get the succ node including or after this id 
Chord::IDMap
LocTable::succ(ConsistentHash::CHID id)
{
  vector<Chord::IDMap> v = succs(id, 1);
  if (size() == 1) {
    return me;
  }
  assert(v.size() > 0);
  return v[0];
  /*
  Time t = now();
  idmapwrap *elm, *elmnext;
  elm = ring.closestsucc(id);
  assert(elm);

  uint rsz = ring.size();
  uint deleted = 0;
  while (1) {

    elmnext = ring.next(elm);
    if (!elmnext) elmnext = ring.first();

    if ((!_timeout) || (t - elm->timestamp < _timeout)) {
      return elm->n;
    }else {
      fprintf(stderr,"%llu me %u,%qx removing %llu %u,%qx\n", t, me.ip,me.id,elm->timestamp,elm->n.ip,elm->n.id);
      ring.remove(elm->id);
      delete elm;
      deleted++;
      elm = elmnext;
      assert(elm->id != me.id);
    }
    assert(rsz - deleted > 1);
  }
  */
}

/* returns m successors including or after the number id*/
vector<Chord::IDMap>
LocTable::succs(ConsistentHash::CHID id, unsigned int m)
{
  //XXX now it is a special case
  assert(id == (me.id + 1));

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

    if (ptr->id == me.id) break; //i should never timeout myself

    if ((!_timeout)||(t - ptr->timestamp) < _timeout) {
      v.push_back(ptr->n);
      j++;
    }else{
      ring.remove(ptr->id);
      delete ptr;
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
      delete elm;
      deleted++;
      elm = elmprev;
    }
    assert((rsz - deleted >= 1));
  }
  assert(elm->n.id == me.id || ConsistentHash::betweenrightincl(me.id, id, elm->n.id));
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
    pos = upper_bound(l.begin(), l.end(), tmppin, Chord::IDMap::cmp) - l.begin();
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
  Chord::IDMap succ1; 
  Chord::IDMap pred1; 

  assert(n.ip > 0 && n.ip < 1025);
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
    elm = new idmapwrap(n,now());
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

  Chord::IDMap succ2 = succ(me.id + 1);
  Chord::IDMap pred2 = pred ();

  if (vis) {
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
  idmapwrap *elm = ring.search(n.id);
  if (!elm) return;
  elm = ring.remove(n.id);
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

    //    printf ("pin %d %16qx %d %d\n", i, pinlist[i].id, pinlist[i].pin_succ,
    //    pinlist[i].pin_pred);
    // find successor of pinlist[i]. XXX don't start at j, but where we left off
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
LocTable::next_hop(Chord::CHID key, bool *done)
{
  *done = false; 
  return pred(key);
}
