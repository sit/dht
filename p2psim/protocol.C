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


void *
Protocol::_doRPC(IPAddress dst, member_f fn, void *args)
{
  Packet *p = new Packet();
  p->_dst = dst;
  p->_src = _node->id();
  p->_c = chancreate(sizeof(Packet*), 0);
  p->_protocol = ProtocolFactory::Instance()->name(this);
  p->_fn = fn;
  p->_args = args;

  send(Network::Instance()->pktchan(), &p);

  // wait for reply. blocking.
  Packet *reply = (Packet *) recvp(p->_c);
  return reply->_args;

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
Protocol::id()
{
  return _node->id();
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
  void *ret = (prot->*packet->_fn)(packet->_args);

  // send reply
  Packet *reply = new Packet();
  IPAddress origsrc = packet->_src;
  reply->_src = prot->id();
  reply->_dst = origsrc;
  reply->_c = packet->_c;
  reply->_protocol = packet->_protocol;
  reply->_args = ret;
  reply->_fn = 0;

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
      insert_doc(e->args);
      break;

    case LOOKUP:
      lookup_doc(e->args);
      break;

    default:
      cerr << "uknown event type " << e->event << endl;
      break;
  }
}
