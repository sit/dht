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
#include "eventqueue.h"
#include "p2psim.h"

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

// Create an RPC packet, send it, and wait for the reply.
// It takes an ordinary function to maximize generality.
// If you want a return value, put it in args.
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
Protocol::_doRPC(IPAddress dst, member_f fn, void* args, void* ret)
{
  Packet *p = new Packet();
  p->_fn = fn;
  p->_args = args;
  p->_ret = ret;
  assert (dst);
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
        // no longer happens, handled in EventQueue and Event
        assert(0);
#if 0
        ap = new pair<Protocol*, Event*>(this, event);
        ThreadManager::Instance()->create(this->_node, Protocol::Dispatch, ap);
#endif
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
      cerr << "unknown event type " << e->event << endl;
      assert(0);
      break;
  }
}
