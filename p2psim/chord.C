#include "chord.h"
#include "node.h"
#include "packet.h"
#include <stdio.h>
#include <iostream>

#define PID(x) (x >> (NBCHID - 32))

#define PAD "000000000000000000000000"

using namespace std;

Chord::Chord(Node *n) : Protocol(n)
{
  me.ip = n->ip();
  me.id = ConsistentHash::ip2chid(me.ip); 
  loctable = new LocTable(me);
  loctable->resize(2+CHORD_SUCC_NUM, CHORD_SUCC_NUM);
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
  sprintf(buf, "%lu Chord(%u,%qu)", now(), me.ip, PID(me.id));
  return buf;
}

void
Chord::lookup(Args *args) 
{
  CHID k = args->nget<CHID>("key");
  vector<IDMap> v = find_successors(k, 1);
  IPAddress ans = (v.size() > 0) ? v[0].ip:0;
  printf("%s lookup results %u\n", ts(), ans);
}

// Returns at least m successors of key.
// This is the lookup() code in Figure 3 of the SOSP03 submission.
// A local call, use find_successors_handler for an RPC.
// Not recursive.
vector<Chord::IDMap>
Chord::find_successors(CHID key, int m)
{
  assert(m <= CHORD_SUCC_NUM);

  IDMap nprime = me;
  int count = 0;
  //printf("%s find_successors key %qu\n", ts(), PID(key));
  while(1){
    assert(count++ < 100);
    next_args na;
    next_ret nr;
    na.key = key;
    na.m = m;
    na.who = me;
    doRPC(nprime.ip, &Chord::next_handler, &na, &nr);
    if(nr.done){
      return nr.v;
    } else {
      /*
      printf("%s !!! node %qu refers to next %qu for key %qu\n",
             ts(), PID(nprime.id), PID(nr.next.id), PID(key));
	     */
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
  IDMap succ = loctable->succ(1);
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
    //ret->next = loctable->next(args->key);
    ret->next = succ;
    assert(ret->next.ip != me.ip);
    /*
    printf("%s next_handler NOT done key=%qu, goto next %qu\n",
           ts(), PID(args->key), PID(ret->next.id));
	   */
  }
}

// External event that tells a node to contact the well-known node
// and try to join.
void
Chord::join(Args *args)
{
  IDMap wkn;
  wkn.ip = args->nget<IPAddress>("wellknown");
  assert (wkn.ip);
  wkn.id = ConsistentHash::ip2chid(wkn.ip);
  loctable->add_node (wkn);

  printf("%s join wellknown %qu\n", ts(), PID(wkn.id));
  Time before = now();
  vector<IDMap> succs = find_successors (me.id + 1, 1);
  assert (succs.size () > 0);
  Time after = now();
  printf("%s join2 %qu, elapsed %ld\n",
         ts(), PID(succs[0].id),
         after - before);
  loctable->add_node(succs[0]);

  //delaycb(STABLE_TIMER, &Chord::stabilize, (void *) 0);
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
  loctable->checkpoint();
  IDMap succ1 = loctable->succ(1);

  if (succ1.ip && (succ1.ip != me.ip)) {
    get_predecessor_args gpa;
    get_predecessor_ret gpr;
    doRPC(succ1.ip, &Chord::get_predecessor_handler, &gpa, &gpr);
    if (gpr.n.ip) loctable->add_node(gpr.n);

    IDMap succ2 = loctable->succ(1);

    if(succ1.id != succ2.id)
      printf("%s changed succ from %qu to %qu\n",
             ts(), PID(succ1.id), PID(succ2.id));

    notify_args na;
    notify_ret nr;
    na.me = me;
    doRPC(succ2.ip, &Chord::notify_handler, &na, &nr);

    fix_predecessor();
    fix_successor();
    if (CHORD_SUCC_NUM > 1) fix_successor_list();
  }else{
    printf("%s stabilize nothing succ (%u,%qu)\n",
           ts(), succ1.ip,PID(succ1.id));
  }
}

bool
Chord::stabilized()
{
  return loctable->stabilized();
}

void
Chord::fix_predecessor()
{
  /*
  IDMap pred = loctable->pred();
  if (pred.ip && Failed(pred.ip)) {
      loctable->del_node(pred);
  }
  */
}

void
Chord::fix_successor()
{
  /*
  IDMap succ = loctable->succ(1);
  if (succ.ip && Failed(succ.ip)) {
    loctable->del_node(succ);
  }
  */
}

void
Chord::get_successor_list_handler(get_successor_list_args *args, get_successor_list_ret *ret)
{
  ret->v = loctable->succs(CHORD_SUCC_NUM);
}


void
Chord::fix_successor_list()
{
  IDMap succ = loctable->succ(1);
  if ((succ.ip == 0) || (succ.ip == me.ip)) return;
  get_successor_list_args gsa;
  get_successor_list_ret gsr;
  doRPC(succ.ip, &Chord::get_successor_list_handler, &gsa, &gsr);
  for (unsigned int i = 0; i < (gsr.v).size(); i++) {
    loctable->add_node(gsr.v[i]);
  }
}


void
Chord::notify_handler(notify_args *args, notify_ret *ret)
{
  IDMap p1 = loctable->pred();
  loctable->notify(args->me);
  IDMap p2 = loctable->pred();
  if(p1.id != p2.id)
    printf("%s notify changed pred from %qu to %qu\n",
         ts(), PID(p1.id), PID(p2.id));

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
  loctable->dump();
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

Chord::IDMap
LocTable::succ(unsigned int which)
{
  assert(which == 1);
  assert(which <= CHORD_SUCC_NUM && which >= 1);
  if (which < (ring.size() -1)) {
    return ring[which];
  }else{
    Chord::IDMap nr;
    nr.ip = 0;
    nr.id = 0;
    return nr;
  }
}

vector<Chord::IDMap>
LocTable::succs(unsigned int m)
{
  assert(m <= CHORD_SUCC_NUM);
  int end = (CHORD_SUCC_NUM > (ring.size()-2))? (ring.size() - 2) : CHORD_SUCC_NUM;
  vector<Chord::IDMap> v;
  v.clear();
  for (int i = 1; i <= end; i++) {
    if (ring[i].ip) {
      v.push_back(ring[i]);
    }
  }
  return v;
}

vector<Chord::IDMap>
LocTable::succ_for_key(Chord::CHID key)
{
  vector<Chord::IDMap> v;
  v.clear();
  int end = (ring.size()-1) > _succ_num ? ring.size() - 2 : _succ_num;
  if (ConsistentHash::between(ring[0].id, ring[end].id, key)) {
    for (int i = 0; i <= end; i++) {
      if (ring[i].ip && ConsistentHash::between(ring[i].id, ring[end].id, key)) 
	v.push_back(ring[i]);
    }
  }
  return v;
}

Chord::IDMap
LocTable::pred()
{
  return ring.back(); //the last element
}

Chord::IDMap
LocTable::pred(Chord::CHID n) {
  //no locality consideration
  for (unsigned int i = 0; i < (ring.size()-1); i++) {
    if ((ring[i+1].ip == 0) || 
	ConsistentHash::between(ring[i].id, ring[i+1].id, n)) {
      return ring[i];
    } 
  }
  assert(0);
  return ring[0];
}

Chord::IDMap
LocTable::next(Chord::CHID n)
{
  //no locality consideration
  unsigned int i;
  for (i = 0; i < (ring.size()-1); i++) {
    if ((ring[i+1].ip == 0) || ConsistentHash::between(ring[i].id, ring[i+1].id, n)) {
      return ring[i];
    } 
  }
  return ring[i+1];
}

void
LocTable::add_node(Chord::IDMap n)
{
  if (!n.ip || (n.ip == ring[0].ip)) return;

  for (unsigned int i = 1; i <= ring.size() ; i++) {

    if (ring[i].ip == n.ip) {
      return;
    } else if (ring[i].ip == 0) {
      ring[i] = n;
      _changed = true;
      return;
    } else if ((i == ring.size()) || ConsistentHash::between(ring[i-1].id, ring[i].id, n.id)) {
      ring.insert(ring.begin() + i, n);
      if (i <= CHORD_SUCC_NUM) {
	_changed = true;
      }else if (pinlist.size() > 0) {
	//is it a better finger candidate than one of my existing fingers?
	for (unsigned int j = 0; j < pinlist.size(); j++) {
	  if (ConsistentHash::betweenleftincl(pinlist[j], pinlist[(j+1)%(pinlist.size())],ring[i].id)) {
	    if (ConsistentHash::betweenrightincl(ring[i-1].id, ring[i].id, pinlist[j])) {
	      _changed = true;
	    }
	  }
	}
      }
      if (ring.size() > _max) {
	evict();
	assert(ring.size() == _max);
      }
      return;
    }
  }
  
}

//can this be part of add_node?
void
LocTable::notify(Chord::IDMap n)
{
  assert(n.ip);
  assert(ring.size() >= 3);
  if ((ring.back().ip == 0) || (ConsistentHash::between(ring.back().id, ring.front().id, n.id))) {
    ring.pop_back();
    ring.push_back(n);
    if (!_changed) _changed = true;
  }
  if ((ring[1].ip == 0) || (ring[1].ip == ring[0].ip)){
    if (!_changed) _changed = true;
    ring[1] = n;
  }
}

void
LocTable::del_node(Chord::IDMap n)
{
  assert(n.ip);
  for (unsigned int i = 0; i <= ring.size(); i++) {
    if (ring[i].ip == n.ip) {
      if (i <= 1 || i == ring.size()) {
	ring[i].ip = 0;
      }else {
	ring.erase(ring.begin() + i); //STL will shift rest of elments leftwise
      }
      return;
    }
  }
}

void
LocTable::pin(Chord::CHID x)
{
  for (unsigned int i = 1; i <= pinlist.size(); i++) {
    if (pinlist[i] == x) {
      return;
    } else if (ConsistentHash::between(pinlist[i-1], pinlist[i], x)) {
      pinlist.insert(pinlist.begin() + i, x);
      return;
    }
  }
  pinlist.push_back(x);
}

unsigned int 
LocTable::evict() //evict one node
{
  int sz;
  sz = pinlist.size();
  if (!pinlist.size()) {
    ring.erase(ring.begin() + _succ_num + 1);
    return (_succ_num + 1);
  }
  assert(0);
  unsigned int j = 0;
  for (unsigned int i = _succ_num + 1; i < ring.size() - 1 ; i++)  {
    while (j < pinlist.size() && ConsistentHash::between(ring[0].id, ring[i].id, pinlist[j])) {
      j++;
    }
    assert(j < pinlist.size() -1);
    if (ConsistentHash::between(pinlist[j], pinlist[j+1], ring[i+1].id)) {
	ring.erase(ring.begin() + i + 1);
	return i + 1;
    }
  }
  assert(0);
  return 0;
}

void
LocTable::checkpoint()
{
  Time tm = now();
  assert(!_prev_chkp || tm >= (_prev_chkp + STABLE_TIMER));
  if (!_changed) {
    _stabilized = true;
  } else {
    _stabilized = false;
  }
  _prev_chkp = tm;
  _changed = false;
}

void
LocTable::dump()
{
  assert(ring.size() >= 3);

  printf("myID is %qx%s %u\n", ring[0].id, PAD, ring[0].ip);

  printf("===== %qx%s =====\n", ring[0].id, PAD);

  for (int i=1; i <= CHORD_SUCC_NUM; i++) {
    printf("%qx%s: succ %d : %qx%s\n",ring[0].id, PAD, i-1,ring[i].id, PAD);
  }

  Chord::CHID prev_finger = 0;
  Chord::CHID pin, tmpr;
  unsigned int i, j;
  bool booltmp;
  j = 0;
  for (i= 0; i < pinlist.size(); i++) {
    pin  = pinlist[i];
    while (1) {
      booltmp = ConsistentHash::betweenrightincl(ring[0].id, ring[j].id, pinlist[i]);
      if (booltmp) break;
      tmpr = ring[j].id;
      //printf(" %qu %qu\n", pin, tmpr);
      j++;
      if (j >= ring.size()) goto END;
    }
    if (ring[j].id != prev_finger) {
      printf("%qx: finger: %d : %qx%s : succ %qx%s\n", ring[0].id, i, pinlist[i], PAD,ring[j].id, PAD);
    }
    prev_finger = ring[j].id;
  }
END:
  int end = ring.size()-1;
  printf("pred : %qx%s\n", ring[end].id, PAD);
}

