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

void *
Chord::find_successor_x(void *x)
{
  printf("Chord(%u,%u)::find_successor_x(%u)\n", me.ip, me.id, (int) x);

  CHID n = (CHID) x;
  /*
  if (recursive) {
    nn = next(n);
    if (nn.ip == me.ip) {
      IDMap *ret = (IDMap*) malloc(sizeof(IDMap)); //this result passing is nasty
      *ret = me;
      return (void *)ret;
    }else{
      return doRPC((IPAddress)successor.ip, Chord::lookup, n, recursive);
    }
  }else{
  */
  IDMap tmp;
  IDMap *nn, *cn;
  cn = &me;
  tmp = next(n);
  nn = &tmp;
  while (nn->ip != cn->ip) {
    cn = nn;
    nn = (IDMap *) doRPC((IPAddress)(cn->ip), Chord::lookup, n);
  }
  IDMap *ret = (IDMap*)malloc(sizeof(IDMap));
  *ret = *cn;
  return (void *)ret;
}

// External event that tells a node to contact the well-known node
// and try to join.
void
Chord::join(Args *args)
{
  IPAddress wkn = (IPAddress) atoi(((*args)["wellknown"]).c_str());

  cout << s() + "::join" << endl;
  void *ret = doRPC(wkn, Chord::find_successor_x, me.id);
  printf("Chord(%u,%u)::join2 %u\n", me.ip, me.id, (CHID) ret);
  loctable->add_node(*(IDMap *)ret);
  free(ret); //nasty RPC results passing
  // stabilize();
}

IDMap
Chord::next(CHID n)
{
  if (ConsistentHash::between(loctable->pred().id, me.id, n)) { 
    return me;
  }else {
    return loctable->succ(1);
  }
}


#if 0

void
Chord::stabilize()
{
  if (successor == 0) return;

  IDMap x = doRPC(successor.ip, get_predecessor);
  if (ConsistentHash::between(me.id, successor.id, x.id)) {
    successor = x;
  }
  doRPC(successor.ip, notify, me);

  //in chord pseudocode, fig 6 of ToN paper, this is a separate periodically called function
  fix_predecessor();
  fix_successor();
}

void
Chord::fix_predecessor()
{
  if (predecessor.ip && Failed(predecessor.ip)) {
    predecessor.ip = 0;
  }
}

void
Chord::fix_successor()
{
  //in the absence of a successor list, 
  // we are doomed
  if (successor.ip && Failed(successor.ip)) {
    successor.ip = 0;
  }
}

void
Chord::notify(IDMap nn)
{
  if ((predecessor.ip == 0) || ConsistentHash::between(predecessor.id, me.id, nn.id)) {
    predecessor = nn;
  }
}

IDMap
Chord::get_predecessor()
{
  return predecessor;
}

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

IDMap
LocTable::succ(unsigned int m)
{
  assert(m == 1);
  //pass only one successor now, should be able to pass m in the future
  return ring[1];
}

IDMap
LocTable::pred()
{
  assert(_end >=2 && _end < _max );
  return ring[_end]; //the end of the ring array contains the predecessor node
}

void
LocTable::add_node(IDMap n)
{

  assert(_end <= _max -1);

  for (int i = 1; i <= _end ; i++) {

    assert(ring[i].ip);

    if (ring[i].ip == n.ip) {
      return;
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
LocTable::notify(IDMap n)
{
  if ((ring[_end].ip == 0) || (ConsistentHash::between(ring[_end].id, ring[0].id, n.id))) {
    ring[_end] = n;
  }
}

void
LocTable::del_node(IDMap n)
{
  int i;

  for (i = 0; i <= _end; i++) {
    if (ring[i].ip == n.ip) {
      break;
    }
  }

  if (i != _end) {
    _end--;
  }

  while ( i < _end ) {
    ring[i] = ring[i+1];
    i++;
  }
  ring[_end].ip = 0;
}

