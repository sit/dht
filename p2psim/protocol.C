#include <assert.h>
#include <lib9.h>
#include <thread.h>
#include <iostream>
#include <stdio.h>
using namespace std;

#include "protocol.h"
#include "protocolfactory.h"
#include "threadmanager.h"
#include "packet.h"
#include "network.h"
#include "p2pevent.h"
#include "node.h"
#include "cbevent.h"
#include "eventqueue.h"
#include "p2psim.h"

Protocol::Protocol(Node *n) : _node(n)
{
  _netchan = chancreate(sizeof(Packet*), 0);
  assert(_netchan);
  _appchan = chancreate(sizeof(P2PEvent*), 0);
  assert(_appchan);
  thread(_node); // run, but map this thread to the node we're running in
}

Protocol::~Protocol()
{
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


bool
Protocol::_doRPC(IPAddress dst, member_f fn, void* args, void* ret)
{
  Packet *p = new Packet();
  p->_fn = fn;
  p->_args = args;
  p->_ret = ret;
  p->_proto = ProtocolFactory::Instance()->name(this);
  return _node->sendPacket(dst, p);
}


IPAddress
Protocol::ip()
{
  return _node->ip();
}



void
Protocol::run()
{
  Alt a[4];
  Packet *packet;
  P2PEvent *event;
  unsigned *exit;
  pair<Protocol*, Event*> *ap;

  a[0].c = _netchan;
  a[0].v = &packet;
  a[0].op = CHANRCV;

  a[1].c = _appchan;
  a[1].v = &event;
  a[1].op = CHANRCV;

  a[2].c = _exitchan;
  a[2].v = &exit;
  a[2].op = CHANRCV;

  a[3].op = CHANEND;
  
  while(1) {
    int i;
    if((i = alt(a)) < 0) {
      cerr << "interrupted" << endl;
      continue;
    }

    switch(i) {
      case 0:
        // packet from network.
        // no longer happens: packets are sent to Nodes.
        assert(0);
        break;

      case 1:
        // application call
        ap = new pair<Protocol*, Event*>();
        ap->first = this;
        ap->second = event;
        ThreadManager::Instance()->create(this->_node, Protocol::Dispatch, ap);
        break;

      case 2:
        // graceful exit
        // cout << "Protocol exit" << endl;
        delete this;

      default:
        break;
    }
  }
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

  delete e;
}
