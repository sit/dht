#include <assert.h>
#include <lib9.h>
#include <thread.h>
#include <iostream>
using namespace std;

#include "p2psim.h"
#include "protocol.h"
#include "protocolfactory.h"
#include "packet.h"
#include "network.h"
#include "p2pevent.h"
#include "node.h"

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


Packet*
Protocol::doRPC(NodeID dst, Packet *p)
{
  p->_dst = dst;
  p->_src = _node->id();
  p->_c = chancreate(sizeof(Packet*), 0);
  p->_protocol = ProtocolFactory::Instance()->name(this);
  assert(p->_c);

  // send it off
  send(Network::Instance()->pktchan(), &p);

  // wait for reply. blocking.
  return (Packet*) recvp(p->_c);
}

NodeID
Protocol::id()
{
  return _node->id();
}


void
Protocol::run()
{
  Alt a[3];
  Packet *packet;
  P2PEvent *e;
  appdispatch_t *ad = 0;
  netdispatch_t *nd = 0;

  a[0].c = _netchan;
  a[0].v = &packet;
  a[0].op = CHANRCV;

  a[1].c = _appchan;
  a[1].v = &e;
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
        nd = new netdispatch_t;
        nd->p = this;
        nd->packet = packet;
        threadcreate(Protocol::Receive, (void*)nd, mainstacksize);
        break;

      case 1:
        // application call
        ad = new appdispatch_t;
        ad->p = this;
        ad->e = e;
        threadcreate(Protocol::Dispatch, (void*)ad, mainstacksize);
        break;

      default:
        break;
    }
  }
}



void
Protocol::Receive(void *p)
{
  netdispatch_t *np = (netdispatch_t*) p;
  Protocol *prot = (Protocol*) np->p;
  Packet *packet = (Packet*) np->packet;

  // do upcall
  Packet *reply = prot->receive((Packet*) np->packet);

  // send reply
  assert(reply);
  NodeID origsrc = packet->_src;
  reply->_src = prot->id();
  reply->_dst = origsrc;
  reply->_c = packet->_c;
  reply->_protocol = packet->_protocol;
  reply->_reply = true;
  send(Network::Instance()->pktchan(), &reply);

  // this is somewhat scary
  delete np;
  threadexits(0);
}


void
Protocol::Dispatch(void *d)
{
  appdispatch_t *dp = (appdispatch_t*) d;
  Protocol *p = (Protocol*) dp->p;
  p->dispatch((P2PEvent*) dp->e);
  delete dp;
  threadexits(0);
}



void
Protocol::dispatch(P2PEvent *e)
{
  switch(e->event) {
    case JOIN:
      join();
      break;

    case LEAVE:
      leave();
      break;

    case CRASH:
      crash();
      break;

    case INSERT:
      insert_doc();
      break;

    case LOOKUP:
      lookup_doc();
      break;

    case STABILIZE:
      stabilize();
      break;

    default:
      cerr << "uknown event type " << e->event << endl;
      break;
  }
}
