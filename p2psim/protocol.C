#include <assert.h>
#include <lib9.h>
#include <thread.h>
#include <iostream>
#include <stdio.h>
using namespace std;

#include "p2psim.h"
#include "protocol.h"
#include "protocolfactory.h"
#include "packet.h"
#include "network.h"
#include "p2pevent.h"
#include "node.h"
#include "cbevent.h"
#include "eventqueue.h"

Protocol::Protocol(Node *n) : _node(n)
{
  _netchan = chancreate(sizeof(Packet*), 0);
  assert(_netchan);
  _appchan = chancreate(sizeof(P2PEvent*), 0);
  assert(_appchan);
  thread();
}

Protocol::~Protocol()
{
}

// Send an RPC packet and wait for the reply.
// in should point to the arguments.
// out should point to a place to put the results.
// The return value indicates whether the RPC succeeded
// (i.e. did not time out).
//
// XXX the intent is that the caller put the args and ret
// on the stack, e.g.
//   foo_args fa;
//   foo_ret fr;
//   fa.xxx = yyy;
//   doRPC(dst, fn, &fa, &fr);
// BUT if the RPC times out, the calling function returns,
// and then the RPC actually completes, we are in trouble.
// It's not even enough to allocate on the heap. We need
// garbage collection, or doRPC needs to know the sizes of
// args/ret and copy them. Even then, naive callers might put
// pointers into the args/ret structures.
bool
Protocol::_doRPC(IPAddress dst, member_f fn, void *args, void *ret)
{
  Packet *p = new Packet();
  p->_dst = dst;
  p->_src = _node->ip();
  p->_c = chancreate(sizeof(Packet*), 0);
  p->_protocol = ProtocolFactory::Instance()->name(this);
  p->_fn = fn;
  p->_args = args;
  p->_ret = ret;

  send(Network::Instance()->pktchan(), &p);

  // wait for reply. blocking.
  Packet *reply = (Packet *) recvp(p->_c);
  assert(reply->_ret == p->_ret);
  return true;

  // Why don't we need to delete the Packet?
}


void
Protocol::_delaycb(Time t, member_f fn, void *args)
{
  CBEvent *e = new CBEvent();
  e->ts = t;
  e->prot = this;
  e->fn = fn;
  e->args = args;
  send(EventQueue::Instance()->eventchan(), (Event**) &e);
}



IPAddress
Protocol::ip()
{
  return _node->ip();
}



void
Protocol::run()
{
  Alt a[3];
  Packet *packet;
  P2PEvent *event;
  pair<Protocol*, Packet*> *np;
  pair<Protocol*, Event*> *ap;

  a[0].c = _netchan;
  a[0].v = &packet;
  a[0].op = CHANRCV;

  a[1].c = _appchan;
  a[1].v = &event;
  a[1].op = CHANRCV;

  a[2].op = CHANEND;
  
  while(1) {
    int i;
    if((i = alt(a)) < 0) {
      cerr << "interrupted" << endl;
      continue;
    }

    switch(i) {
      case 0:
        // packet from network
        np = new pair<Protocol*, Packet*>();
        np->first = this;
        np->second = packet;
        threadcreate(Protocol::Receive, (void*)np, mainstacksize);
        break;

      case 1:
        // application call
        ap = new pair<Protocol*, Event*>();
        ap->first = this;
        ap->second = event;
        threadcreate(Protocol::Dispatch, (void*)ap, mainstacksize);
        break;

      default:
        break;
    }
  }
}



void
Protocol::Receive(void *p)
{
  pair<Protocol*, Packet*> *np = (pair<Protocol*, Packet*> *) p;
  Protocol *prot = (Protocol*) np->first;
  Packet *packet = (Packet*) np->second;

  // do upcall using the function pointer in the packet. yuck.
  (prot->*packet->_fn)(packet->_args, packet->_ret);

  // send reply
  Packet *reply = new Packet();
  IPAddress origsrc = packet->_src;
  reply->_src = prot->ip();
  reply->_dst = origsrc;
  reply->_c = packet->_c;
  reply->_protocol = packet->_protocol;
  reply->_args = 0;
  reply->_ret = packet->_ret;
  reply->_fn = 0;        // indicates that this is a RPC reply

  send(Network::Instance()->pktchan(), &reply);

  // this is somewhat scary
  delete np;
  threadexits(0);
}


void
Protocol::Dispatch(void *p)
{
  pair<Protocol*, Event*> *ap = (pair<Protocol*, Event*>*) p;
  Protocol *prot = (Protocol*) ap->first;
  prot->dispatch((P2PEvent*) ap->second);
  delete ap;
  threadexits(0);
}



void
Protocol::dispatch(P2PEvent *e)
{
  switch(e->event) {
    case JOIN:
      join(e->args);
      break;

    case LEAVE:
      leave(e->args);
      break;

    case CRASH:
      crash(e->args);
      break;

    case INSERT:
      insert(e->args);
      break;

    case LOOKUP:
      lookup(e->args);
      break;

    default:
      cerr << "uknown event type " << e->event << endl;
      break;
  }
}
