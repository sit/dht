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
          ThreadManager::Instance()->create(Node::Receive, p);
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


//
// sendPacket should only be used by
//
bool
Node::sendPacket(IPAddress dst, Packet *p)
{
  p->_src = ip();
  p->_dst = dst;

  // where to send the reply
  Channel *c = p->_c = chancreate(sizeof(Packet*), 0);

  // send it off. blocks, but Network reads constantly
  send(Network::Instance()->pktchan(), &p);

  // block on reply
  Packet *reply = (Packet *) recvp(c);

  chanfree(c);
  delete reply;
  delete p;

  return true;
}


//
// Receive is only invoked for the first half of the RPC.  The reply goes
// directly to the appropriate channel.
//
void
Node::Receive(void *px)
{
  Packet *p = (Packet *) px;
  Node *n = Network::Instance()->getnode(p->dst());

  // get pointer to protocol
  Protocol *proto = n->getproto(p->_proto);

  // invoke function call
  // send it up to the protocol
  (proto->*(p->_fn))(p->_args, p->_ret);

  // make reply
  Packet *reply = new Packet();
  reply->_c = p->_c;
  reply->_src = p->_dst;
  reply->_dst = p->_src;
  reply->_ret = p->_ret; // the reply for the layer above

  // send it back
  send(Network::Instance()->pktchan(), &reply);

  // ...and we're done
  threadexits(0);
}
