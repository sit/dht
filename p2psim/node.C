#include <iostream>
using namespace std;

#include <assert.h>
#include <stdio.h>

#include "protocolfactory.h"
#include "threadmanager.h"
#include "node.h"
#include "packet.h"
#include "network.h"
#include "protocol.h"

Node::Node(IPAddress ip) : _ip(ip), _pktchan(0)
{
  _pktchan = chancreate(sizeof(Packet*), 0);
  assert(_pktchan);
  _protchan = chancreate(sizeof(string), 0);
  assert(_protchan);
  thread();
}

Node::~Node()
{
  chanfree(_pktchan);
  chanfree(_protchan);
  _protmap.clear();
}


void
Node::run()
{
  Alt a[4];
  Packet *p;
  string protname;
  Protocol *prot;
  unsigned exit;

  a[0].c = _pktchan;
  a[0].v = &p;
  a[0].op = CHANRCV;

  a[1].c = _protchan;
  a[1].v = &protname;
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
        // if this is a reply, send it back on the channel where the thread is
        // waiting a reply.  otherwise call the function.
        if(p->reply()){
          send(p->channel(), &p);
        } else {
          pair<Node*, Packet*> px = make_pair(this, p);
          ThreadManager::Instance()->create(this, Node::Receive, &px);
        }
        break;

      // add protocol to node
      case 1:
        prot = ProtocolFactory::Instance()->create(protname, this);
        assert(prot);
        if(_protmap[protname]) {
          cerr << "warning: " << protname << " already running on node " << ip() << endl;
          delete _protmap[protname];
        }
        _protmap[protname] = prot;
        break;

      //exit
      case 2:
        // cout << "Node exit" << endl;
        for(PMCI p = _protmap.begin(); p != _protmap.end(); ++p)
          send(p->second->exitchan(), 0);
        _protmap.clear();
        delete this;


      default:
        break;
    }
  }
}

// Send an RPC packet and wait for the reply.
// It takes an ordinary function to maximize generality.
// If you want a return value, put it in args.
// See rpc.h/doRPC for a nice interface.
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
Node::sendPacket(IPAddress dst, Packet *p)
{
  // find source IP address.
  // Node *srcnode = (Node*) ThreadManager::Instance()->get(threadid());
  // assert(srcnode);
  // IPAddress srca = srcnode->ip();
  p->_src = ip();
  p->_dst = dst;

  // where to send the reply
  Channel *c = p->_c = chancreate(sizeof(Packet*), 0);

  // send it off.
  send(Network::Instance()->pktchan(), &p);

  // block on reply
  Packet *reply = (Packet *) recvp(c);

  chanfree(c);
  delete reply;
  delete p;

  return true;
}

void
Node::Receive(void *px)
{
  pair<Node*, Packet*> *pr = (pair<Node*, Packet*>*) px;
  Packet *p = pr->second;

  // get pointer to protocol
  Protocol *proto = pr->first->getproto(p->_proto);

  // invoke function call
  // send it up to the protocol
  (proto->*(p->_fn))(p->_args, p->_ret);

  // send reply
  Packet *reply = new Packet();
  reply->_c = p->_c;
  reply->_src = p->_dst;
  reply->_dst = p->_src;
  reply->_ret = p->_ret; // contains the reply

  // send it back
  send(Network::Instance()->pktchan(), &reply);

  // ...and we're done
  threadexits(0);
}

Protocol *
Node::getproto(const type_info &ti)
{
  string dstprotname = ProtocolFactory::Instance()->name(ti);
  return getproto(dstprotname);
}
