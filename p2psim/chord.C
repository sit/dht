#include "chord.h"
#include "node.h"
#include "packet.h"
#include <stdio.h>
#include <iostream>
#include <algorithm>

#define PID(x) (x >> (NBCHID - 32))


using namespace std;

Chord::Chord(Node *n, uint numsucc) : Protocol(n)
{
  nsucc = numsucc;
  me.ip = n->ip();
  me.id = ConsistentHash::ip2chid(me.ip); 
  loctable = new LocTable(me);
  //pin down nsucc for successor list
  loctable->pin(me.id + 1, true, nsucc);
  //pin down 1 predecessor
  loctable->pin(me.id - 1, false, 1);
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
  printf("%s lookup key %qx\n", ts (), k);
  vector<IDMap> v = find_successors(k, 1, false);
  IPAddress ans = (v.size() > 0) ? v[0].ip:0;
  printf("%s lookup results (%u,%qx)\n", ts(), ans, (ans != 0) ? v[0].id : 0);
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
  IDMap succ = loctable->succ(me.id+1);
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
  vector<IDMap> succs = find_successors (me.id + 1, 1, true);
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
 
  IDMap succ1 = loctable->succ(me.id+1);

  if (succ1.ip && (succ1.ip != me.ip)) {
    get_predecessor_args gpa;
    get_predecessor_ret gpr;
    doRPC(succ1.ip, &Chord::get_predecessor_handler, &gpa, &gpr);
    if (gpr.n.ip) loctable->add_node(gpr.n);

    IDMap succ2 = loctable->succ(me.id+1);

    /*
    if(succ1.id != succ2.id)
      printf("%s changed succ from %qu to %qu\n",
             ts(), PID(succ1.id), PID(succ2.id));
	     */

    notify_args na;
    notify_ret nr;
    na.me = me;
    doRPC(succ2.ip, &Chord::notify_handler, &na, &nr);

    fix_predecessor();
    fix_successor();
    if (nsucc > 1) fix_successor_list();
  }
}

bool
Chord::stabilized(vector<CHID> lid)
{
  vector<CHID>::iterator iter;
  iter = find(lid.begin(), lid.end(), me.id);
  assert(iter != lid.end());

  vector<IDMap> succs = loctable->succs(me.id+1, nsucc);
  if (succs.size() != nsucc) 
    return false;

  for (unsigned int i = 1; i <= nsucc; i++) {
    iter++;
    if (iter == lid.end()) iter = lid.begin();
    if (succs[i-1].id != *iter) return false;
  }
  return true;
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
  IDMap succ = loctable->succ(me.id+1);
  if (succ.ip && Failed(succ.ip)) {
    loctable->del_node(succ);
  }
  */
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
  printf("myID is %qx%s %u\n", me.id, PAD, me.ip);
  printf("===== %qx%s =====\n", me.id, PAD);

  vector<IDMap> v = loctable->succs(me.id+1, nsucc);
  for (unsigned int i = 0; i < v.size(); i++) {
    printf("%qx%s: succ %d : %qx%s\n", me.id, PAD, i,v[i].id, PAD);
  }
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
  // XXX: shouldn't the just be a new?
  ring.clear();
  ring.push_back(me);//ring[0] is always me
  
  _max = 0;
  pinlist.clear();
  pin(me.id, true, 1);

} 

//get the succ node including or after this id 
Chord::IDMap
LocTable::succ(ConsistentHash::CHID id)
{
  for (unsigned i = 0; i < ring.size(); i++) {
    if (ConsistentHash::betweenrightincl(ring[0].id, ring[i].id, id)) 
      return ring[i];
  }
  Chord::IDMap nr;
  nr.ip = 0;
  nr.id = 0;
  return nr;
}

/* returns m successors including or after the number id*/
vector<Chord::IDMap>
LocTable::succs(ConsistentHash::CHID id, unsigned int m)
{
  unsigned int num = m;
  vector<Chord::IDMap> v;
  v.clear();
  for (unsigned int i = 0; i < ring.size(); i++) {
    if (ConsistentHash::betweenrightincl(ring[0].id, ring[i].id, id)) {
      v.push_back(ring[i]);
      num--;
      if (num <= 0) return v;
    }
  }
  return v;
}

vector<Chord::IDMap>
LocTable::succ_for_key(Chord::CHID key)
{
  vector<Chord::IDMap> v;
  v.clear();
  /*
  int end = (ring.size()-1) > _succ_num ? ring.size() - 2 : _succ_num;
  if (ConsistentHash::between(ring[0].id, ring[end].id, key)) {
    for (int i = 0; i <= end; i++) {
      if (ring[i].ip && ConsistentHash::between(ring[i].id, ring[end].id, key)) 
	v.push_back(ring[i]);
    }
    }*/
  return v;
}

Chord::IDMap
LocTable::pred()
{
  Chord::IDMap pred = ring.back();
  if (pred.ip == ring[0].ip) {
    pred.ip = 0;
  }
  return pred;
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
  return ring.back();  // n must be between (last, first]
}

void
LocTable::add_node(Chord::IDMap n)
{
  if (!n.ip || (n.ip == ring[0].ip)) return;

  for (unsigned int i = 1; i < ring.size() ; i++) {

    if (ring[i].ip == n.ip) {
      return;
    } else if (ring[i].ip == 0) {
      ring[i] = n;
      return;
    } else if (ConsistentHash::between(ring[i-1].id, ring[i].id, n.id)) {
      ring.insert(ring.begin() + i, n);
      if (ring.size() > _max) {
	evict();
	assert(ring.size() <= _max);
      }
      return;
    }
  }
  notify(n);
}

//can this be part of add_node?
void
LocTable::notify(Chord::IDMap n)
{
  assert(n.ip);
  //  assert(ring.size() >= 3);
  if ((ring.back().ip == 0) || (ConsistentHash::between(ring.back().id, ring.front().id, n.id))) {
    //ring.pop_back();
    ring.push_back(n);
    if (ring.size() > _max) {
      evict();
    }
  }
  /*
  if ((ring[1].ip == 0) || (ring[1].ip == ring[0].ip)){
    ring[1] = n;
    }*/
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
LocTable::pin(Chord::CHID x, bool pin_succ, unsigned int pin_num)
{
  pin_entry pe;

  pe.id = x;
  pe.pin_succ = pin_succ;
  pe.pin_num = pin_num;

  _max += pin_num;
  for (unsigned int i = 0; i < pinlist.size(); i++) {
    if (pinlist[i].id == x) {
      if (pin_succ == pinlist[i].pin_succ) {
	pinlist[i].pin_num = pin_num;
      }else if (!pin_succ) { //pin the predecessor
	pinlist.insert(pinlist.begin() + i, pe);
      }else {
	pinlist.insert(pinlist.begin() + i + 1, pe);
      }
      return;
    } else if (i > 0 && ConsistentHash::between(pinlist[i-1].id, pinlist[i].id, x)) {
      pinlist.insert(pinlist.begin() + i, pe);
      return;
    }
  }
  pinlist.push_back(pe);
}

//in its full generality, evict has to loop around the list of nodes 3 times whic is 
// expensive
unsigned int 
LocTable::evict() //all unnecessary(unpinned) nodes 
{
  assert(pinlist.size() < _max && pinlist.size() > 0);
  assert(pinlist[0].id == ring[0].id);

  vector<bool> pinned; 

  unsigned int sz = ring.size();

  for (unsigned int i = 0; i < sz; i++) {
    pinned.push_back(false);
  }

  int extra;
  bool end = false;
  int j = 0;
  int tmp;
  for (unsigned int i = 0; i < pinlist.size(); i++) {
    while ((!end) && j < ring.size()) {
      if (ConsistentHash::betweenrightincl(ring[0].id, ring[j].id, pinlist[i].id))
        break;
      j++;
    }
    if (j >= sz) {
      j = 0;
      end = true;
    }
    if (pinlist[i].pin_succ) {
      for (unsigned int k = 0; k < pinlist[i].pin_num; k++) {
	if ((j + k) < sz)
	  pinned[j + k] = true;
      }
    } else {
      //since ring[j].id is always equal or after pinlist[i].id
      extra = (pinlist[i].id == ring[j].id)? 0:-1;

      //since i walk around the pinlist and ring in closewise fashion, i allow wrapping around (i.e. the % operation) for pinning predecessors
      for (unsigned int k = 0; k < pinlist[i].pin_num; k++) {
        tmp = (j - k + extra + sz) % sz;
        pinned[tmp] = true;
      }
    } 
  }

  unsigned int i = 0;
  while (i < pinned.size()) {
    if (!pinned[i]) {
      pinned.erase(pinned.begin() + i);
      ring.erase(ring.begin() + i);
    }else{
      i++;
    }
  }
  assert(ring.size() <= _max);
}

