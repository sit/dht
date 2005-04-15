/*
 * Copyright (c) 2003-2005 Thomer M. Gil (thomer@csail.mit.edu),
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
#include <cmath>
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

  _all_ips = New vector<IPAddress>;
  assert(_all_ips);
  _all_nodes = New set<Node*>;
  assert(_all_nodes);

  _highest_ip = 0;
  _changed = false;

  // get the nodes
  thread();
}


Network::~Network()
{
  for(HashMap<IPAddress, Node*>::const_iterator p = _nodes.begin(); p != _nodes.end(); ++p)
    delete p.value();
  chanfree(_nodechan);
  delete _top;
  delete _failure_model;
  delete _all_ips;
  delete _all_nodes;
}


const set<Node*> *
Network::getallnodes()
{
  // copy only non-mapped IP address
  if(!_all_nodes->size())
    for(HashMap<IPAddress, Node*>::const_iterator p = _nodes.begin(); p != _nodes.end(); ++p)
      _all_nodes->insert(p.value());

  return _all_nodes;
}

vector<IPAddress> *
Network::getallfirstips()
{
  if(!_all_ips->size())
    for(HashMap<IPAddress, Node*>::const_iterator p = _nodes.begin(); p != _nodes.end(); ++p) {
      _all_ips->push_back(p.key());
    }

  _changed = false;
  return _all_ips;
}

Time
Network::avglatency()
{
  static Time answer = 0;
  Time total_latency = 0;
  unsigned n = 0;

  if(answer)
    return answer;

  if(_nodes.size() == 0 || _nodes.size() == 1)
    return (answer = 0);

  for(HashMap<IPAddress, Node*>::const_iterator p = _nodes.begin(); p != _nodes.end(); ++p) {
    for(HashMap<IPAddress, Node*>::const_iterator q = _nodes.begin(); q != _nodes.end(); ++q) {
      if(p.key() == q.key())
        continue;
      total_latency += _top->latency(p.key(), q.key(), true);
      total_latency += _top->latency(p.key(), q.key(), false);
      n += 2;
    }
  }

  return (answer = (total_latency / n));
}


// 
// taken from http://www-math.mit.edu/~spielman/ECC/randGen.c, which notes the
// source as the book "Numerical Recipies in C".
// 
float
Network::gaussian(double variance)
{
  float fac,rsq,v1,v2;

  if(variance == 0)
    return 0.0;

  do {
    v1=2.0*(random()/(RAND_MAX*1.0))-1.0;
    v2=2.0*(random()/(RAND_MAX*1.0))-1.0;
    rsq = v1*v1 + v2*v2;
  } while (rsq >= 1.0 || rsq == 0.0);
  fac = sqrt(-2.0*log(rsq)/rsq);

  return (v2*fac) * (variance);
}


IPAddress
Network::unused_ip()
{
  return ++_highest_ip;
}




// Protocols should call send() to send a packet into the network.
void
Network::send(Packet *p)
{
  extern bool with_failure_model;

  Node *dst = getnode(p->dst());
  Node *src = getnode(p->src());
  assert (dst);
  assert (src);

  Time latency = _top->latency(src->ip(), dst->ip(), p->reply());
  if (src->ip () != dst->ip ())
    latency += p->_queue_delay;

  //
  // if the packet was still ok, see if it accidentally disappears due to
  // lossrate.
  //
  // if the node was dead or the packet got lost, we have to delay the packet a
  // bit. (p->ok is set on the receiving side, so if it's false, this must be a
  // reply. punish the packet by delaying it according to the failure model.)
  //
  // if timeout value was passed to doRPC(), then doRPC has to return
  // [timeout]ms after it was started.  make sure we return by then.
  //
  // if timeout == 0, then let the failure model ADD some punishment.
  //
  if(p->ok() && _top->lossrate()) {
    unsigned random_number = (unsigned) ((random() % 10000));
    p->_ok = _top->lossrate() <= random_number ? true: false;
  }

  if(!p->ok()) {
    if(p->timeout()) {
      int tmplat = p->timeout() - _top->latency(dst->ip(), src->ip(), false);
      latency = tmplat <= 0 ? 0 : (unsigned) tmplat;
    } else if(with_failure_model) {
      latency += _failure_model->failure_latency(p);
    }
  }

  NetEvent *ne = New NetEvent();
  assert(ne);
  Time tmplat = (Time) (latency + 
			gaussian(latency*(_top->noise_variance()/100.0)));
  ne->ts = now() + tmplat;
  ne->ip = p->dst();
  ne->p = p;
  EventQueue::Instance()->add_event(ne);
}


void
Network::map_ip(IPAddress firstx, IPAddress newx)
{
  assert(!_new2old.find_pair(newx));
  _new2old.insert(newx, firstx);
}

IPAddress
Network::first_ip(IPAddress newx)
{
  if(_new2old.find_pair(newx))
    return _new2old[newx];
  assert(newx <= _nodes.size());
  return newx;
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
        if(_nodes[node->ip()]) {
          cout << "warning: node " << node->ip() << " already exists" << endl;
          _nodes.remove(node->ip());
        }

        _nodes.insert(node->ip(), node);

        if(node->ip() > _highest_ip)
          _highest_ip = node->ip();

        _changed = true;
        break;

      default:
        break;
    }
  }
}

#include "bighashmap.cc"
