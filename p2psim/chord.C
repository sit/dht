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

#define CHORD_DEBUG
Chord::Chord(Node *n, uint numsucc, LocTable *l)
  : DHTProtocol(n), _isstable (false)
{
  _vivaldi = NULL;
  nsucc = numsucc;
  me.ip = n->ip();
  me.id = ConsistentHash::ip2chid(me.ip); 
  if (l) 
    loctable = l;
  else
    loctable = new LocTable();

  loctable->init (me);

  //pin down nsucc for successor list
  loctable->pin(me.id + 1, nsucc, 0);
  //pin down 1 predecessor
  loctable->pin(me.id - 1, 0, 1);

  if (vis) {
    printf ("vis %llu node %16qx\n", now (), me.id);
  }
  _inited = false;
}

Chord::~Chord()
{
  cout << "done " << me.ip << " " << me.id << endl;
  delete loctable;
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
  uint recurs = args->nget<int>("recurs");
  Time begin = now();
  Topology *t = Network::Instance()->gettopology();
  if (recurs) {
    IDMap v = find_successors_recurs(k, false);
    //    uint lat = t->latency(me.ip, v.ip);
  } else {
    vector<IDMap> v = find_successors(k, 1, false); 
    uint lat = v.size()>0? t->latency(me.ip, v[0].ip):0;
    printf("%s lookup %16qx results (%u,%16qx) interval %u %llu\n", ts(),k, v.size() > 0 ?v[0].ip:0, v.size() > 0 ? v[0].id:0, 2 * lat, now() - begin);
  }
}

void
Chord::find_successors_handler(find_successors_args *args, find_successors_ret *ret)
{
  check_static_init();
  ret->v = find_successors(args->key, args->m, true);
}

// Returns at least m successors of key.
// This is the lookup() code in Figure 3 of the SOSP03 submission.
// A local call, use find_successors_handler for an RPC.
// Not recursive.
vector<Chord::IDMap>
Chord::find_successors(CHID key, uint m, bool intern)
{
  assert(m <= nsucc);

  IDMap nprime = me;
  int count = 0;

  vector<IDMap> route;

  //printf("%s find_successors key %qu\n", ts(), PID(key));

  if (vis && !intern) 
    printf ("vis %llu search %16qx %16qx\n", now(), me.id, key);

  next_args na;
  next_ret nr;

  na.key = key;
  na.m = m;

#ifdef CHORD_DEBUG
  CHID diff = key - me.id;
#endif

  while(1){
    assert(count++ < 5000);
    if (vis && !intern) 
      printf ("vis %llu step %16qx %16qx\n", now(), me.id, nprime.id);

    
    bool r;
    if (_vivaldi) {
      Chord *target = dynamic_cast<Chord *>(getpeer(nprime.ip));
      r = _vivaldi->doRPC(nprime.ip, target, &Chord::next_handler, &na, &nr);
    } else
      r = doRPC(nprime.ip, &Chord::next_handler, &na, &nr);

    if(r && nr.done){
      route.push_back(nr.v[0]);
#ifdef CHORD_DEBUG
      printf("%s find_successor %qx route: ",ts(), key);
      for (unsigned int i = 0; i < route.size(); i++) {
	CHID tmpdiff = key - route[i].id;
	printf("(%u,%qx %.5f) ", route[i].ip, route[i].id, ((double)tmpdiff)/((double) diff));
      }
      printf("\n");
#endif

      //actually talk to the successor
      if (_vivaldi) {
	Chord *target = dynamic_cast<Chord *>(getpeer(nr.v[0].ip));
	r = _vivaldi->doRPC(nr.v[0].ip, target, &Chord::null_handler, (void *)NULL, (void *)NULL);
      } else
	r = doRPC(nr.v[0].ip, &Chord::null_handler, (void *)NULL, (void *)NULL);

      if (vis && !intern) 
	printf ("vis %llu step %16qx %16qx\n", now(), me.id, nr.v[0].id);

      break;
    } else if (r) {
#ifdef CHORD_DEBUG
      CHID newdiff = key - nr.next.id;
      assert(ConsistentHash::between(me.id, key, nr.next.id));
      assert(newdiff < diff);
#endif
      route.push_back(nr.next);
      nprime = nr.next;
    } else {
      printf ("%16qx rpc to %16qx failed %llu\n", me.id, nprime.id, now ());
      route.pop_back ();
      if (route.size () > 0) {
	alert_args aa;
	alert_ret ar;
	aa.n = nprime;
	nprime = route.back ();
	doRPC(nprime.ip, &Chord::alert_handler, &aa, &ar);
      } else {
	nr.v.clear ();
	break; 
      }
    }
  }

  if (!intern) {
    printf ("find_successor for (id %qx, key %qx)", me.id, key);
    if (nr.v.size () == 0) {
      printf ("failed\n");
    } else {
      printf ("is (%u, %qx) hops %d %d\n", nr.v[0].ip, nr.v[0].id, count+1, route.size());
    }
  }
  return nr.v;
}

Chord::IDMap 
Chord::find_successors_recurs(CHID key, bool intern)
{
  next_recurs_args fa;
  next_recurs_ret fr;
  fa.v.clear();
  fa.v.push_back(me);
  fa.key = key;

  //  Time before = now();
  doRPC(me.ip, &Chord::next_recurs_handler, &fa, &fr);
  IDMap succ = fr.v.back();
  if (!intern) {
    printf("find_successors_recurs for (id %qx, key %qx) is (%u, %qx) hops %d\n", me.id, key, succ.ip, succ.id, fr.v.size());
  }
#ifdef CHORD_DEBUG
  Topology *t = Network::Instance()->gettopology();
  printf("%s find_successors_recurs %qx route ",ts(),key);
  uint total_lat = 0;
  for (uint i = 0; i < fr.v.size(); i++) {
    IDMap n = fr.v[i];
    printf("(%u,%qx,%u) ", n.ip, n.id, i < (fr.v.size()-1)? 2 * t->latency(fr.v[i].ip, fr.v[i+1].ip): 0);
    if (i < (fr.v.size() - 3)) {
      total_lat += 2 * t->latency(fr.v[i].ip,fr.v[i+1].ip);
    }
  }
  //  uint interval = now() - before;
  //assert(interval == total_lat);
  printf("\n");

  printf("%s lookup %16qx results (%u,%16qx) interval %u %u\n", ts(), key, succ.ip, succ.id, 2 * t->latency(me.ip, succ.ip), total_lat);

#endif
  return succ;
}
void
Chord::next_recurs_handler(next_recurs_args *args, next_recurs_ret *ret)
{
  check_static_init();

  IDMap succ = loctable->succ(me.id+1);
  if(ConsistentHash::betweenrightincl(me.id, succ.id, args->key) ||
      succ.id == me.id) {
    ret->v = args->v;
    ret->v.push_back(succ);
  }else{

    bool r = false;
    next_recurs_args nargs;
    next_recurs_ret nret;

    nargs.key = args->key;
    nargs.v = args->v;

    while (!r) {
      IDMap next = loctable->next_hop(args->key);
      nargs.v.push_back(next);

      if (_vivaldi) {
	Chord *target = dynamic_cast<Chord *>(getpeer(next.ip));
	r = _vivaldi->doRPC(next.ip, target, &Chord::next_recurs_handler, &nargs, &nret);
      } else
	r = doRPC(next.ip, &Chord::next_recurs_handler, &nargs, &nret);

      if (!r) {
	printf ("%16qx rpc to %16qx failed %llu\n", me.id, next.id, now ());
	printf ("vis %llu delete %16qx %16qx\n", now (), me.id, next.id);
	nargs.v.pop_back();
	loctable->del_node(next);
      }
    }

    ret->v = nret.v;
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
    ret->done = true;
    ret->v.clear();
    ret->v.push_back(succ);
 } else {
    ret->done = false;
    ret->next = loctable->next_hop(args->key);
    assert(ret->next.ip != me.ip);
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

  int dim = args->nget<int>("model-dimension", 10);
  if (dim <= 0) {
    cerr << "dimension must be specified (and positive)\n";
    exit (0);
  }

  _vivaldi = new Vivaldi10(node(), 3, 0.05, 1); 

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
  bool ok = doRPC(wkn.ip, &Chord::find_successors_handler, &fa, &fr);
  assert (ok);
  assert(fr.v.size() > 0);
  Time after = now();
  printf("%s join2 %16qx, elapsed %llu\n",
         ts(), fr.v[0].id,
         after - before);
  loctable->add_node(fr.v[0]);

  reschedule_stabilizer(NULL);
}


void
Chord::reschedule_stabilizer(void *x)
{
  //  if (!_isstable) {
    stabilize();
    delaycb(STABLE_TIMER, &Chord::reschedule_stabilizer, (void *) 0);
    //  }
}

// Which paper is this code from? -- PODC 
void
Chord::stabilize()
{
  IDMap succ1 = loctable->succ(me.id+1);

  fix_successor ();
  succ1 = loctable->succ(me.id+1);

  notify_args na;
  notify_ret nr;
  na.me = me;
  if (_vivaldi) {
    Chord *target = dynamic_cast<Chord *>(getpeer(succ1.ip));
    _vivaldi->doRPC(succ1.ip, target, &Chord::notify_handler, &na, &nr);
  } else
    doRPC(succ1.ip, &Chord::notify_handler, &na, &nr);


  if (nsucc > 1) fix_successor_list();

  //vivaldi random lookups
#ifdef RANDOM_LOOKUPS
  CHID random_key = ConsistentHash::getRandID ();
  vector<Chord::IDMap> ignore = find_successors (random_key, 1, false);
#endif
  
}

bool
Chord::stabilized(vector<CHID> lid)
{
  vector<CHID>::iterator iter;
  iter = find(lid.begin(), lid.end(), me.id);
  assert(iter != lid.end());

  vector<IDMap> succs = loctable->succs(me.id+1, nsucc);

#if 0
  printf ("stable? successor list %u,%16qx at %lu\n", me.ip, me.id, now ());
  for (unsigned int i = 0; i < succs.size (); i++) {
    printf (" successor %d: %u, %16qx\n", i, succs[i].ip, succs[i].id);
  }
#endif

  if (succs.size() != nsucc) 
    return false;

  for (unsigned int i = 1; i <= nsucc; i++) {
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
  printf("Chord: %s inited %d %d\n", ts(), ids.size(), loctable->size());
  _inited = true;
}

void
Chord::fix_successor()
{
  IDMap succ1 = loctable->succ(me.id+1);

  get_predecessor_args gpa;
  get_predecessor_ret gpr;
  bool ok;
  if (_vivaldi) {
    Chord *target = dynamic_cast<Chord *>(getpeer(succ1.ip));
    ok = _vivaldi->doRPC(succ1.ip, target, &Chord::get_predecessor_handler, &gpa, &gpr);
  } else
    ok = doRPC(succ1.ip, &Chord::get_predecessor_handler, &gpa, &gpr);

  assert (ok);

#ifdef CHORD_DEBUG
  printf("%s fix_successor old successor (%u,%qx)'s predecessor is (%u, %qx)\n", ts(), succ1.ip, succ1.id, gpr.n.ip, gpr.n.id);
#endif

  if (gpr.n.ip) loctable->add_node(gpr.n);
}

void
Chord::get_successor_list_handler(get_successor_list_args *args, get_successor_list_ret *ret)
{
  assert(!static_sim);
  ret->v = loctable->succs(me.id+1, nsucc);
}


void
Chord::fix_successor_list()
{
  IDMap succ = loctable->succ(me.id+1);
  get_successor_list_args gsa;
  get_successor_list_ret gsr;
  bool ok;
  if (_vivaldi) {
    Chord *target = dynamic_cast<Chord *>(getpeer(succ.ip));
    ok = _vivaldi->doRPC(succ.ip, target, &Chord::get_successor_list_handler, &gsa, &gsr);
  } else
    ok = doRPC(succ.ip, &Chord::get_successor_list_handler, &gsa, &gsr);

  assert (ok);
  for (unsigned int i = 0; i < (gsr.v).size(); i++) {
    loctable->add_node(gsr.v[i]);
  }

  // printf ("fix_successor_list: %u,%16qx at %lu succ %u,%16qx\n", me.ip, me.id, 
  //now(), succ.ip, succ.id);
  
  if (vis) {
    bool change = false;

    vector<IDMap> scs = loctable->succs(me.id + 1, nsucc);
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

  vector<IDMap> v = loctable->succs(me.id+1, nsucc);
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
  printf ("vis %llu crash %16qx\n", now (), me.id);
  node()->crash ();
}


/*************** LocTable ***********************/

LocTable::LocTable() 
{
  rsz = 0;
} 

void LocTable::init (Chord::IDMap me)
{
  myid = me.id;
  pin(me.id, 1, 0);
  idmapwrap *elm = new idmapwrap(me);

  ring.insert(elm);
  rsz = 1;
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
  idmapwrap *elm;
  elm = ring.closestsucc(id);
  assert(elm);
  return elm->n;
}

/* returns m successors including or after the number id*/
vector<Chord::IDMap>
LocTable::succs(ConsistentHash::CHID id, unsigned int m)
{
  vector<Chord::IDMap> v;
  v.clear();

  if (m <= 0) return v;
  idmapwrap *elm = ring.closestsucc(id);
  idmapwrap *ptr = elm;
  for (uint j = 0; j < m; j++) {
    v.push_back(ptr->n);
    ptr = ring.next(ptr);
    if (ptr == elm) break; //if the ring does not have m elements
    if (!ptr) {
      ptr = ring.first(); //wrap around
    }
  }
  return v;
}

Chord::IDMap
LocTable::pred()
{
  return pred(myid-1);
}

Chord::IDMap
LocTable::pred(Chord::CHID id) 
{
  idmapwrap *elm = ring.closestpred(id);
  assert(elm);
  assert(elm->n.id == myid || ConsistentHash::betweenrightincl(myid, id, elm->n.id));
  return elm->n;
}

void
LocTable::print ()
{
  idmapwrap *me = ring.search(myid);
  assert(me);
  printf ("ring:\n");
  idmapwrap *i = me;
  do {
    printf ("  %5u,%16qx\n", i->n.ip, i->n.id);
    i = ring.next(i);
    if (!i) i = ring.first();
  }while (i != me);
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

  assert (ring.repok ());
  if (vis) {
    succ1 = succ(myid+1);
    pred1 = pred();
  }

  idmapwrap *elm = new idmapwrap(n);
  if (ring.insert(elm)) {
  }else{
    delete elm;
    return;
  }

  assert (ring.repok ());

  if (_evict && ring.size() > _max) {
    evict();
  }

  assert (ring.repok ());
  Chord::IDMap succ2 = succ(myid + 1);
  Chord::IDMap pred2 = pred ();

  if (vis) {
    if(succ1.id != succ2.id) {
      printf("vis %llu succ %16qx %16qx\n", now (), myid, succ2.id);
    }

    if(pred1.id != pred2.id) {
      printf("vis %llu pred %16qx %16qx\n", now (), myid, pred2.id);
    }
  }
}

void
LocTable::del_node(Chord::IDMap n)
{
  idmapwrap *elm = ring.search(n.id);
  assert(elm); //just a check
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
  assert(pinlist.size() <= _max);
  assert(pinlist.size() > 0);


  idmapwrap *ptr;
  idmapwrap *elm = ring.first();
  for (uint i = 0; i < rsz; i++) {
    elm->pinned = false;
    elm = ring.next(elm);
  }
  assert(elm == NULL);

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
  assert(rsz == size());
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
      rsz--;
    } 
    elm = next;
  }

  uint tmpsz = size();
  assert(tmpsz == rsz);
  assert(elm == NULL);
}

Chord::IDMap
LocTable::next_hop(Chord::CHID key)
{
  return pred(key);
}
