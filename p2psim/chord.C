#include "chord.h"
#include "node.h"
#include "packet.h"
#include <stdio.h>
#include <iostream>


using namespace std;

Chord::Chord(Node *n) : Protocol(n)
{
  me.id = ConsistentHash::getRandID(); 
  me.ip = n->ip();
  loctable = new LocTable(me);
}

Chord::~Chord()
{
  printf("done %d %u\n",
         me.ip, me.id);
  delete loctable;
}

string
Chord::s()
{
  char buf[50];
  sprintf(buf, "Chord(%u,%u)", me.ip, me.id);
  return string(buf);
}

// Returns at least m successors of key.
// This is the lookup() code in Figure 3 of the SOSP03 submission.
// A local call, use find_successors_handler for an RPC.
// Not recursive.
vector<Chord::IDMap>
Chord::find_successors(CHID key, int m)
{
  // assert(m <= successor list length);

  IDMap nprime = me;
  int count = 0;
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
  IDMap succ = loctable->succ(args->m);
  printf("Chord(%u) next key=%u succ=%u\n",
         me.id, args->key, succ.id);
  if(ConsistentHash::between(me.id, succ.id, args->key) ||
     succ.id == me.id){
    ret->done = true;
    ret->v.clear();
    ret->v.push_back(succ);
  } else {
    ret->done = false;
    ret->next = succ;
  }
}

// Handle a find_successor RPC.
// Only used for Chord's internal maintenance (e.g. join).
// DHash &c should use find_successors.
void
Chord::find_successor_handler(find_successor_args *args,
                              find_successor_ret *ret)
{
  printf("Chord(%u,%u)::find_successor_handler(%u)\n",
         me.ip, me.id, args->n);

  vector<Chord::IDMap> sl = find_successors(args->n, 1);
  assert(sl.size() > 0);
  ret->succ = sl[0];
}

// External event that tells a node to contact the well-known node
// and try to join.
void
Chord::join(Args *args)
{
  IPAddress wkn = args->nget<IPAddress>("wellknown");
  assert(wkn);
  cout << s() + "::join" << endl;
  Time before = now();
  find_successor_args fsa;
  find_successor_ret fsr;
  fsa.n = me.id;
  doRPC(wkn, &Chord::find_successor_handler, &fsa, &fsr);
  Time after = now();
  printf("Chord(%u,%u)::join2 %u, elapsed %ld\n",
         me.ip, me.id, fsr.succ.id,
         after - before);
  loctable->add_node(fsr.succ);
  // stabilize();
}

void
Chord::stabilize()
{
  Chord::IDMap succ = loctable->succ(1);
  if (succ.ip == 0) return;

  get_predecessor_args gpa;
  get_predecessor_ret gpr;
  doRPC(succ.ip, &Chord::get_predecessor_handler, &gpa, &gpr);
  loctable->add_node(gpr.n);

  succ = loctable->succ(1);
  notify_args na;
  notify_ret nr;
  na.me = me;
  doRPC(succ.ip, &Chord::notify_handler, &na, &nr);

  //in chord pseudocode, fig 6 of ToN paper, this is a separate periodically called function
  fix_predecessor();
  fix_successor();
}

void
Chord::fix_predecessor()
{
  /*
  Chord::IDMap pred = loctable->pred();
  if (pred.ip && Failed(pred.ip)) {
      loctable->del_node(pred);
  }
  */
}

void
Chord::fix_successor()
{
  /*
  Chord::IDMap succ = loctable->succ(1);
  if (succ.ip && Failed(succ.ip)) {
    loctable->del_node(succ);
  }
  */
}

void
Chord::notify_handler(notify_args *args, notify_ret *ret)
{
  loctable->notify(args->me);
}

void
Chord::get_predecessor_handler(get_predecessor_args *args,
                               get_predecessor_ret *ret)
{
  ret->n = loctable->pred();
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
LocTable::succ(unsigned int m)
{
  assert(m == 1);
  //pass only one successor now, should be able to pass m in the future
  return ring[1];
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
	ConsistentHash::between(n, ring[i].id, ring[i+1].id)) {
      return ring[i];
    } 
  }
  assert(0);
}

Chord::IDMap
LocTable::next(Chord::CHID n)
{
  //no locality consideration
  for (unsigned int i = 0; i < (ring.size()-1); i++) {
    if ((ring[i+1].ip == 0) || ConsistentHash::between(ring[i].id, ring[i+1].id, n)) {
      return ring[i];
    } 
  }
  assert(0);
}

void
LocTable::add_node(Chord::IDMap n)
{
  int end = ring.size() -1;
  for (int i = 1; i < end ; i++) {

    if (ring[i].ip == n.ip) {
      return;
    } else if (ring[i].ip == 0) {
      assert(i == 1);
      ring[i] = n;
    } else if (ConsistentHash::between(ring[i-1].id, ring[i].id, n.id)) {

      ring.insert(ring.begin() + i, n);
      if (ring.size() > _max) {
	evict();
      }
      return;
    }
  }
}

//can this be part of add_node?
void
LocTable::notify(Chord::IDMap n)
{
  if ((ring.back().ip == 0) || (ConsistentHash::between(ring.back().id, ring.front().id, n.id))) {
    ring.pop_back();
    ring.push_back(n);
  }
}

void
LocTable::del_node(Chord::IDMap n)
{
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
  for (int i = 1; i <= pinlist.size(); i++) {
    if (pinlist[i] == x) {
      return;
    } else if (ConsistentHash::between(pinlist[i-1], pinlist[i], x)) {
      pinlist.insert(pinlist.begin() + i, x);
      return;
    }
  }
  pinlist.push_back(x);
}

void
LocTable::evict() //evict one node
{
  int j = 0;
  for (int i = _succ_num + 1; i < ring.size(); i++)  {
    while (ConsistentHash::between(ring[0].id, pinlist[j],ring[i].id)) {
      j++;
      assert(j < pinlist.size());
    }
    if (ConsistentHash::between(ring[i].id, pinlist[j], ring[i+1].id)) {
	ring.erase(ring.begin() + i);
	return;
    }
  }
  assert(0);
}
