#include "threadmanager.h"
#include "network.h"
#include "protocols/protocolfactory.h"
#include <iostream>
#include <cassert>
using namespace std;

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

//
// The network has just delivered a packet to a Node.
// If it's an RPC reply, send to channel of waiting caller.
// If it's an RPC request, start a new thread to handle it.
//
void
Node::got_packet(Packet *p)
{
  extern bool with_failure_model;

  if(!p->reply()){
    // if this failed, then ask the failure model how long to delay it, and
    // push it back to the network to drift around there for a bit.
    ThreadManager::Instance()->create(Node::Receive, p);
    return;
  }

  // IF we're running with failure_models
  // if this packet failed to arrive then delay it a bit before it arrives
  if(with_failure_model && !p->ok() && !p->punished()) {
    send(Network::Instance()->failedpktchan(), &p);
    p->punish();
    return;
  }


  // packet is a reply and has been punished for its failure according to some
  // failure model. so now it can finally arrive.
  send(p->channel(), &p);
}

void
Node::run()
{
  Alt a[2];
  Packet *p;

  a[0].c = _pktchan;
  a[0].v = &p;
  a[0].op = CHANRCV;

  a[1].op = CHANEND;

  while(1) {
    int i;
    if((i = alt(a)) < 0) {
      cerr << "interrupted" << endl;
      continue;
    }

    switch(i) {
      case 0:
        got_packet(p);
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

  // where to send the reply, buffered for single reply
  Channel *c = p->_c = chancreate(sizeof(Packet*), 1);

  // send it off. blocks, but Network reads constantly
  send(Network::Instance()->pktchan(), &p);
  return New RPCHandle(c, p);
}


bool
Node::_doRPC_receive(RPCHandle *rpch)
{
  Packet *reply = (Packet *) recvp(rpch->channel());
  bool ok = reply->_ok;
  delete reply;
  delete rpch;
  return ok;
}



//
// Node::run() invokes Receive() when an RPC request arrives.
// The reply goes back directly to the appropriate channel.
//
void
Node::Receive(void *px)
{
  Packet *p = (Packet *) px;
  Node *n = Network::Instance()->getnode(p->dst());
  assert(n);

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
