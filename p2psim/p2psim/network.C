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

#include "network.h"
#include "events/netevent.h"
#include "failuremodels/failuremodelfactory.h"
#include <iostream>
#include <cassert>
using namespace std;

Network *Network::_instance = 0;

Network*
Network::Instance(Topology *top, FailureModel *fm)
{
  if(!_instance)
    _instance = New Network(top, fm);
  return _instance;
}


Network::Network(Topology *top, FailureModel *fm) : _top(0),
  _nodechan(0)
{
  _nodechan = chancreate(sizeof(Node*), 0);
  assert(_nodechan);
  _top = top;
  assert(_top);
  _failure_model = fm;
  assert(_failure_model);

  // get the nodes
  thread();
}


Network::~Network()
{
  for(NMCI p = _nodes.begin(); p != _nodes.end(); ++p)
    delete p->second;
  chanfree(_nodechan);
  delete _top;
}


list<Protocol*>
Network::getallprotocols(string proto)
{
  list<Protocol*> pl; // XXX: should we just New this?  return may be expensive

  for(NMCI p = _nodes.begin(); p != _nodes.end(); ++p)
    pl.push_back(p->second->getproto(proto));
  return pl;
}

list<IPAddress>
Network::getallips()
{
  list<IPAddress> il; // XXX: should we just New this? return may be expensive

  for(NMCI p = _nodes.begin(); p != _nodes.end(); ++p)
    il.push_back(p->first);
  return il;
}

// Protocols should call here() to send a packet into the network.
void
Network::send(Packet *p)
{
  extern bool with_failure_model;

  Node *dstnode = _nodes[p->dst()];
  Node *srcnode = _nodes[p->src()];
  assert (dstnode);
  assert (srcnode);

  Time latency = _top->latency(srcnode->ip(), dstnode->ip());

  // p->ok is set on the receiving side, so if it's false, this must be a
  // reply.  punish the packet by delaying it according to the failure model.
  if(with_failure_model && !p->ok())
    latency += _failure_model->failure_latency(p);

  NetEvent *ne = New NetEvent();
  assert(ne);
  ne->ts = now() + latency;
  ne->node = dstnode;
  ne->p = p;
  EventQueue::Instance()->add_event(ne);
}

void
Network::run()
{
  Alt a[3];
  Node *node;

  a[0].c = _nodechan;
  a[0].v = &node;
  a[0].op = CHANRCV;

  a[1].op = CHANEND;
  
  while(1) {
    int i;

    if((i = alt(a)) < 0) {
      cerr << "interrupted" << endl;
      continue;
    }

    switch(i) {
      // register node on network
      case 0:
        if(_nodes[node->ip()])
          cerr << "warning: " << node->ip() << " already in network" << endl;
        _nodes[node->ip()] = node;
        break;

      default:
        break;
    }
  }
}
