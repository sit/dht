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
}

string
Chord::s()
{
  char buf[50];
  sprintf(buf, "Chord(%u,%u)", me.ip, me.id);
  return string(buf);
}

// Handle a find_successor RPC.
void
Chord::find_successor_handler(find_successor_args *args,
                              find_successor_ret *ret)
{
  Chord::CHID n = args->n;

  printf("Chord(%u,%u)::find_successor_x(%u)\n", me.ip, me.id, (int) n);

#if 1
  ret->succ = me;
#else
  /*
  if (recursive) {
    nn = loctable->next(n);
    if (nn.ip == me.ip) {
      ret->succ = me;
      return;
    }else{
      return doRPC((IPAddress)successor.ip, Chord::lookup, n, recursive);
    }
  }else{
  */
  Chord::IDMap tmp;
  Chord::IDMap *nn, *cn;
  cn = &me;
  tmp = loctable->next(n);
  nn = &tmp;
  while (nn->ip != cn->ip) {
    cn = nn;
    struct lookup_args la;
    struct lookup_ret lr;
    la.key = n;
    // Hmm, what is lookup? Why not find_successor?
    doRPC((cn->ip), Chord::lookup_handler, &la, &lr);
    nn = lr.succ;
  }
  ret->succ = *cn;
#endif
}

// External event that tells a node to contact the well-known node
// and try to join.
void
Chord::join(Args *args)
{
  IPAddress wkn = (IPAddress) atoi(((*args)["wellknown"]).c_str());
  assert(wkn);
  cout << s() + "::join" << endl;
  find_successor_args fsa;
  find_successor_ret fsr;
  fsa.n = me.id;
  doRPC(wkn, Chord::find_successor_handler, &fsa, &fsr);
  printf("Chord(%u,%u)::join2 %u\n", me.ip, me.id, fsr.succ.ip);
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
  doRPC(succ.ip, Chord::get_predecessor_handler, &gpa, &gpr);
  loctable->add_node(gpr.n);

  succ = loctable->succ(1);
  notify_args na;
  notify_ret nr;
  na.me = me;
  doRPC(succ.ip, Chord::notify_handler, &na, &nr);

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
  assert(_end >=2 && _end < _max );
  return ring[_end]; //the end of the ring array contains the predecessor node
}

Chord::IDMap
LocTable::next(Chord::CHID n)
{
  //no locality consideration
  for (int i = 0; i < _end; i++) {
    if ((ring[i+1].ip == 0) || ConsistentHash::between(ring[i].id, ring[i+1].id, n)) {
      return ring[i];
    } 
  }
  assert(0);
}

void
LocTable::add_node(Chord::IDMap n)
{

  assert(_end <= _max -1);

  for (int i = 1; i < _end ; i++) {

    if (ring[i].ip == n.ip) {
      return;
    } else if (ring[i].ip == 0) {
      assert(i == 1);
      ring[i] = n;
    } else if (ConsistentHash::between(ring[i-1].id, ring[i].id, n.id)) {

      if (_end < (_max -1)) {
	//there is space to add more node in the ring
	for (int j = _end; j >= i; j--) {
	  ring[j+1] = ring[j];
	}
	ring[i] = n;
	_end++;
	return;

      } else {
	if (i <= _succ_num) {
	  //evict the last one of the _succ_num succecessors??
	  for (int j = _succ_num; j > i; j++) {
	    ring[j] = ring[j-1];
	  }
	  ring[i] = n;
	}else{
	  //what should this eviction policy be? i do not know yet, just throw away this node for the moment
	}
      }
      return;
    }
  }
}

//can this be part of add_node?
void
LocTable::notify(Chord::IDMap n)
{
  if ((ring[_end].ip == 0) || (ConsistentHash::between(ring[_end].id, ring[0].id, n.id))) {
    ring[_end] = n;
  }
}

void
LocTable::del_node(Chord::IDMap n)
{
  int i;

  for (i = 0; i <= _end; i++) {
    if (ring[i].ip == n.ip) {
      break;
    }
  }

  if ((i == _end) || (_end <= 2)) {
    ring[i].ip = 0; 
    return;
  }

  while ( i < (_end-1)) {
    ring[i] = ring[i+1];
    i++;
  }
  ring[_end].ip = 0;
  _end--;
}

