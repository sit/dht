#include "chord.h"
#include "node.h"
#include "packet.h"
#include <stdio.h>
#include <iostream>
#include <algorithm>

using namespace std;
extern bool vis;

Chord::Chord(Node *n, uint numsucc) : Protocol(n)
{
  nsucc = numsucc;
  me.ip = n->ip();
  me.id = ConsistentHash::ip2chid(me.ip); 
  loctable = new LocTable(me);
  //pin down nsucc for successor list
  loctable->pin(me.id + 1, nsucc, 0);
  //pin down 1 predecessor
  loctable->pin(me.id - 1, 0, 1);

  if (vis) {
    printf ("vis %lu node %16qx\n", now (), me.id);
  }
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
  sprintf(buf, "%lu Chord(%5u,%16qx)", now(), me.ip, me.id);
  return buf;
}

void
Chord::lookup(Args *args) 
{
  CHID k = args->nget<CHID>("key");
  printf("%s lookup key %16qx\n", ts (), k);
  vector<IDMap> v = find_successors(k, 1, false);
  IPAddress ans = (v.size() > 0) ? v[0].ip:0;
  printf("%s lookup results (%u,%16qx)\n", ts(), ans, (ans != 0) ? v[0].id : 0);
}

void
Chord::find_successors_handler(find_successors_args *args, find_successors_ret *ret)
{
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

#ifdef CHORD_DEBUG
  vector<IDMap> route;
#endif
  //printf("%s find_successors key %qu\n", ts(), PID(key));

  if (vis && !intern) 
    printf ("vis %lu search %16qx %16qx\n", now(), me.id, key);

  while(1){
    assert(count++ < 100);
    next_args na;
    next_ret nr;
    na.key = key;
    na.m = m;

    if (vis && !intern) 
       printf ("vis %lu step %16qx %16qx\n", now(), me.id, nprime.id);

    doRPC(nprime.ip, &Chord::next_handler, &na, &nr);
    if(nr.done){
#ifdef CHORD_DEBUG
      route.push_back(nr.v[0]);
      printf("%s find_successor %qx route: ",ts(), key);
      for (unsigned int i = 0; i < route.size(); i++) {
	printf("(%u,%qx) ", route[i].ip, route[i].id);
      }
      printf("\n");
#endif

    if (vis && !intern) 
       printf ("vis %lu step %16qx %16qx\n", now(), me.id, nr.v[0].id);

      return nr.v;
    } else {
#ifdef CHORD_DEBUG
      route.push_back(nr.next);
#endif
      nprime = nr.next;
    }
  }
}

// From Figure 3 of SOSP03 submission.
// Returns either m successors of the given key,
// or the node to talk to next.
void
Chord::next_handler(next_args *args, next_ret *ret)
{
  /*
  vector<IDMap> v = loctable->succ_for_key(args->key);
  if (v.size() >= args->m) {
    printf("%s next_handler key=%u, %u succs, %u\n",
           ts(), args->key, v.size(), v[0].id);
    */
  //IDMap succ = loctable->succ(args->m);
  IDMap succ = loctable->succ(me.id+1);

  /* The condition loctable->size()!=2 is a terrible hack. 
     When a node first joins the system,
     two entries (me and the wellknown node) are inserted into loctable
     but we do not want to mis-use either as the successor answer during find_successor*/
  if(ConsistentHash::betweenrightincl(me.id, succ.id, args->key) ||
     succ.id == me.id){
    ret->done = true;
    ret->v.clear();
    ret->v.push_back(succ);
    /*
    printf("%s next_handler done key=%qu succ=%qu\n",
           ts(), PID(args->key), PID(succ.id));
	   */
 }else {
    ret->done = false;
    ret->next = loctable->pred(args->key);
    //ret->next = succ;
    assert(ret->next.ip != me.ip);
 }
}

// External event that tells a node to contact the well-known node
// and try to join.
void
Chord::join(Args *args)
{
  if (vis) {
    printf("vis %lu join %16qx\n", now (), me.id);
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
  doRPC(wkn.ip, &Chord::find_successors_handler, &fa, &fr);
  assert(fr.v.size() > 0);
  Time after = now();
  printf("%s join2 %16qx, elapsed %ld\n",
         ts(), fr.v[0].id,
         after - before);
  loctable->add_node(fr.v[0]);

  reschedule_stabilizer(NULL);
}

void
Chord::reschedule_stabilizer(void *x)
{
  stabilize();
  delaycb(STABLE_TIMER, &Chord::reschedule_stabilizer, (void *) 0);
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
  doRPC(succ1.ip, &Chord::notify_handler, &na, &nr);

  if (nsucc > 1) fix_successor_list();
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
      printf("%s not stablized, %5d succ should be %16qx instead of (%u, %16qx)\n", ts(), i-1, *iter, succs[i-1].ip,  succs[i-1].id);
      return false;
    }
  }

  return true;
}

void
Chord::init_state(vector<IDMap> ids)
{
  printf("%s inited %d\n", ts(), ids.size());
  for (vector<IDMap>::iterator iter = ids.begin (); iter < ids.end (); 
       ++iter) {
    loctable->add_node(*iter);
  }
}

void
Chord::fix_successor()
{
  IDMap succ1 = loctable->succ(me.id+1);

  get_predecessor_args gpa;
  get_predecessor_ret gpr;
  doRPC(succ1.ip, &Chord::get_predecessor_handler, &gpa, &gpr);

#ifdef CHORD_DEBUG
  printf("%s fix_successor old successor (%u,%qx)'s predecessor is (%u, %qx)\n", ts(), succ1.ip, succ1.id, gpr.n.ip, gpr.n.id);
#endif

  if (gpr.n.ip) loctable->add_node(gpr.n);
}

void
Chord::get_successor_list_handler(get_successor_list_args *args, get_successor_list_ret *ret)
{
  ret->v = loctable->succs(me.id+1, nsucc);
}


void
Chord::fix_successor_list()
{
  IDMap succ = loctable->succ(me.id+1);
  get_successor_list_args gsa;
  get_successor_list_ret gsr;
  doRPC(succ.ip, &Chord::get_successor_list_handler, &gsa, &gsr);
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
      printf ( "vis %lu succlist %16qx", now (), me.id);
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
  IDMap p1 = loctable->pred();
  loctable->add_node(args->me);
}

void
Chord::get_predecessor_handler(get_predecessor_args *args,
                               get_predecessor_ret *ret)
{
  ret->n = loctable->pred();
}

void
Chord::dump()
{
  printf("myID is %16qx %5u\n", me.id, me.ip);
  printf("===== %16qx =====\n", me.id);

  vector<IDMap> v = loctable->succs(me.id+1, nsucc);
  for (unsigned int i = 0; i < v.size(); i++) {
    printf("%16qx: succ %5d : %16qx\n", me.id, i, v[i].id);
  }
  IDMap p = loctable->pred();
  printf("pred is %5u,%16qx\n", p.ip, p.id);
}

#if 0
void
Chord::leave()
{
}

void
Chord::crash()
{
  //suspend the currently running thread?
}
#endif




/*************** LocTable ***********************/

LocTable::LocTable(Chord::IDMap me) {
  pin(me.id, 1, 0);
  ring.push_back (me); //ring[0] is always me
} 

uint
LocTable::findsuccessor (ConsistentHash::CHID x)
{
  uint r = 0;
  for (uint i = 0; i < ring.size(); i++) {
    if (ConsistentHash::betweenrightincl(ring[0].id, ring[i].id, x)) {
      r = i;
      break;
    }
  }
  // printf ("ring: findsuccessor %16qx is ring %d %16qx\n", x, r, ring[r].id);
  return r;
}


//get the succ node including or after this id 
Chord::IDMap
LocTable::succ(ConsistentHash::CHID id)
{
  uint i = findsuccessor (id);
  return ring[i];
}

/* returns m successors including or after the number id*/
vector<Chord::IDMap>
LocTable::succs(ConsistentHash::CHID id, unsigned int m)
{
  vector<Chord::IDMap> v;
  v.clear();

  if (m <= 0) return v;
  uint i = findsuccessor (id);
  m = (m > ring.size ()) ? ring.size () : m;
  for (uint j = 0; j < m; j++) {
    v.push_back(ring[(i + j) % ring.size ()]);
  }
  return v;
}

Chord::IDMap
LocTable::pred()
{
  Chord::IDMap pred = ring.back();
  return pred;
}

Chord::IDMap
LocTable::pred(Chord::CHID n) {
  uint i = findsuccessor (n);
  Chord::IDMap p;
  if (i == 0) p = ring.back ();
  else p = ring[i-1];
  return p;
}

void
LocTable::print ()
{
  printf ("ring:\n");
  for (uint i = 0; i < ring.size (); i++) {
    printf ("  %5u,%16qx\n", ring[i].ip, ring[i].id);

  }
}

void
LocTable::add_node(Chord::IDMap n)
{
  Chord::IDMap succ1 = (vis == true) ? succ(ring[0].id + 1) : ring[0];
  Chord::IDMap pred1 = (vis == true) ? pred () : ring[0];

  if (ring.size () == 0) {
    ring.push_back (n);
  } else {
    uint i = findsuccessor (n.id);
    if (ring[i].id != n.id) {
      if ((ring.size () == 1) || (i == 0)) {
	ring.push_back (n);
      } else {
        ring.insert(ring.begin() + i, n);
      }
    }
  }

  assert ((pinlist.size () == 0) || (ring[0].id == pinlist[0].id));

  if (ring.size() > _max) {
    evict();
    assert(ring.size() <= _max);
  }

  Chord::IDMap succ2 = succ(ring[0].id + 1);
  Chord::IDMap pred2 = pred ();

  if (vis) {
    if(succ1.id != succ2.id) {
      printf("vis %lu succ %16qx %16qx\n", now (), ring[0].id, succ2.id);
    }

    if(pred1.id != pred2.id) {
      printf("vis %lu pred %16qx %16qx\n", now (), ring[0].id, pred2.id);
    }
  }
}

void
LocTable::del_node(Chord::IDMap n)
{
  for (unsigned int i = 0; i < ring.size(); i++) {
    if (ring[i].id == n.id) {
      ring.erase(ring.begin() + i); //STL will shift rest of elments leftwise
    }
    return;
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

  assert (pinlist[0].id == ring[0].id);

  uint i = 0;
  for (; i < pinlist.size(); i++) {
    if (ConsistentHash::betweenrightincl(pinlist[0].id, pinlist[i].id, x)) {
      break;
    }
  }

  if (pinlist[i].id == x) {
    pinlist[i].pin_pred = pin_pred;
    pinlist[i].pin_succ = pin_succ;
  } else {
    if (pinlist.size () == 1) {
      pinlist.push_back(pe);
    } else {
      pinlist.insert(pinlist.begin() + i, pe);
    }
  }

  assert (pinlist[0].id == ring[0].id);
  assert(pinlist.size() <= _max);
}


void
LocTable::evict() // all unnecessary(unpinned) nodes 
{
  assert(pinlist.size() <= _max);
  assert(pinlist.size() > 0);
  assert(pinlist[0].id == ring[0].id);

  vector<bool> pinned; 

  for (unsigned int i = 0; i < ring.size (); i++) {
    pinned.push_back(false);
  }

  uint i = 0; // index into pinlist
  uint j = 0; // index into ring
  while (i < pinlist.size ()) {

    //    printf ("pin %d %16qx %d %d\n", i, pinlist[i].id, pinlist[i].pin_succ,
    //    pinlist[i].pin_pred);

    // find successor of pinlist[i]. XXX don't start at j, but where we left off
    for ( ; j < ring.size (); j++) {
      if (ConsistentHash::betweenrightincl(ring[0].id, ring[j].id, 
					   pinlist[i].id))
	break;
    }
    if (j == ring.size ()) j = 0;

    // pin the predecessors
    for (unsigned int k = 0; k < pinlist[i].pin_pred; k++) {
      int v =  j - 1 - k;
      int p =  (v > 0) ? v : (v + ring.size ());
      pinned[p] = true;
    }

    // pin the successors, starting with j
    for (unsigned int k = 0; k < pinlist[i].pin_succ; k++) {
      pinned[(j + k) % ring.size ()] = true;
    }

    i++;
  }

  // evict entries that are not pinned
  j = 0;
  for (uint i = 0; i < pinned.size (); i++) {
    if (!pinned[i]) {
      ring.erase(ring.begin() + j);
    } else {
      j++;
    }
  }
  assert(ring.size() <= _max);
}

