#include <iostream>
#include <set>
#include <string>
using namespace std;

#include <assert.h>
#include <stdio.h>

#include "protocolfactory.h"
#include "threadmanager.h"
#include "node.h"
#include "packet.h"
#include "network.h"
#include "protocol.h"
#include "rpchandle.h"

Node::Node(IPAddress ip) : _ip(ip), _alive (true), _pktchan(0)
{
  _pktchan = chancreate(sizeof(Packet*), 0);
  assert(_pktchan);

  // add all the protocols
  set<string> allprotos = ProtocolFactory::Instance()->getnodeprotocols();
  for(set<string>::const_iterator i = allprotos.begin(); i != allprotos.end(); ++i) {
    Protocol *prot = ProtocolFactory::Instance()->create(*i, this);
    register_proto(prot);
  }

  thread();
}

Node::~Node()
{
  for(PMCI p = _protmap.begin(); p != _protmap.end(); ++p)
    delete p->second;
  _protmap.clear();
  chanfree(_pktchan);
}


void
Node::register_proto(Protocol *p)
{
  string name = p->proto_name();
  if(_protmap[name]){
    cerr << "warning: " << name << " already running on node " << ip() << endl;
    delete _protmap[name];
  }
  _protmap[name] = p;
}

void
Node::run()
{
  Alt a[3];
  Packet *p;
  unsigned exit;

  a[0].c = _pktchan;
  a[0].v = &p;
  a[0].op = CHANRCV;

  a[1].c = _exitchan;
  a[1].v = &exit;
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
        // if this is a reply, send it back on the channel where the thread is
        // waiting a reply.  otherwise call the function.
        if(p->reply()){
          send(p->channel(), &p);
        } else {
          ThreadManager::Instance()->create(Node::Receive, p);
        }
        break;

      //exit
      case 1:
        // cout << "Node exit" << endl;
        delete this;
        break;


      default:
        break;
    }
  }
}


//
// Send off a request packet asking Node::Receive to
// call fn(args), wait for reply.
// Return value indicates whether we received a reply,
// i.e. absence of time-out.
//
bool
Node::_doRPC(IPAddress dst, void (*fn)(void *), void *args)
{
  return _doRPC_receive(_doRPC_send(dst, fn, 0, args));
}


RPCHandle*
Node::_doRPC_send(IPAddress dst, void (*fn)(void *), void (*killme)(void *), void *args)
{
  Packet *p = New Packet;
  p->_fn = fn;
  p->_killme = killme;
  p->_args = args;
  p->_src = ip();
  p->_dst = dst;

  // where to send the reply
  Channel *c = p->_c = chancreate(sizeof(Packet*), 0);

  // send it off. blocks, but Network reads constantly
  send(Network::Instance()->pktchan(), &p);
  return New RPCHandle(c, p);
}


bool
Node::_doRPC_receive(RPCHandle *rpch)
{
  // block on reply
  Packet *reply = (Packet *) recvp(rpch->channel());
  bool ok = reply->_ok;
  delete reply;
  delete rpch;
  return ok;
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

  // make reply
  Packet *reply = New Packet;
  reply->_c = p->_c;
  reply->_src = p->_dst;
  reply->_dst = p->_src;

  if (n->alive ()) {
    (p->_fn)(p->_args);
    reply->_ok = true;
  } else {
    reply->_ok = false;  // XXX delete reply for timeout?
  }

  // send it back
  send(Network::Instance()->pktchan(), &reply);

  // ...and we're done
  threadexits(0);
}
