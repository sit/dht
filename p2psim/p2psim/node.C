/*
 * Copyright (c) 2003 Thomer M. Gil (thomer@csail.mit.edu),
 *                    Robert Morris (rtm@csail.mit.edu).
 *                    Massachusetts Institute of Technology
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

#include "threadmanager.h"
#include "network.h"
#include "protocols/protocolfactory.h"
#include <iostream>
#include <cassert>
using namespace std;

Node::Node(IPAddress ip) : _ip(ip), _alive (true)
{
  // add all the protocols
  set<string> allprotos = ProtocolFactory::Instance()->getnodeprotocols();
  for(set<string>::const_iterator i = allprotos.begin(); i != allprotos.end(); ++i) {
    Protocol *prot = ProtocolFactory::Instance()->create(*i, this);
    register_proto(prot);
  }
}

Node::~Node()
{
  for(PMCI p = _protmap.begin(); p != _protmap.end(); ++p)
    delete p->second;
  _protmap.clear();
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

// Called by NetEvent::execute() to deliver a packet to a Node,
// after Network processing (i.e. delays and failures).
void
Node::got_packet(Packet *p)
{
  if(p->reply()){
    // RPC reply, give to waiting thread.
    send(p->channel(), &p);
  } else {
    // RPC request, start a handler thread.
    ThreadManager::Instance()->create(Node::Receive, p);
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

  Network::Instance()->send(p);

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
// Node::got_packet() invokes Receive() when an RPC request arrives.
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

  // send it back, potentially with a latency punishment for when this node was
  // dead.
  Network::Instance()->send(reply);

  // ...and we're done
  threadexits(0);
}
