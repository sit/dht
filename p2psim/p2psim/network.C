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


Network::Network(Topology *top, FailureModel *fm) : _top(0), _pktchan(0),
  _failedpktchan(0), _nodechan(0)
{
  _pktchan = chancreate(sizeof(Packet*), 0);
  assert(_pktchan);
  _failedpktchan = chancreate(sizeof(Packet*), 0);
  assert(_failedpktchan);
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
  chanfree(_pktchan);
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



void
Network::run()
{
  Alt a[4];
  Packet *p;
  Node *node;
  NetEvent *ne;
  Time latency;

  a[0].c = _pktchan;
  a[0].v = &p;
  a[0].op = CHANRCV;

  a[1].c = _failedpktchan;
  a[1].v = &p;
  a[1].op = CHANRCV;

  a[2].c = _nodechan;
  a[2].v = &node;
  a[2].op = CHANRCV;

  a[3].op = CHANEND;
  
  while(1) {
    int i;

    if((i = alt(a)) < 0) {
      cerr << "interrupted" << endl;
      continue;
    }

    Node *dstnode, *srcnode;
    switch(i) {
      // get packet from network and schedule delivery
      case 0:
        // first time this packet goes onto the network, touch it.
        if(!p->touched())
          p->touch();
        dstnode = _nodes[p->dst()];
	srcnode = _nodes[p->src()];
	assert (dstnode);
	assert (srcnode);
        latency = _top->latency(srcnode->ip(), dstnode->ip());
        ne = New NetEvent();
        assert(ne);
        ne->ts = now() + latency;
        ne->node = dstnode;
        ne->p = p;
        EventQueue::Instance()->here(ne);
        break;
    
      // get failed packet from network and let it linger around for a bit.
      case 1:
	assert (p->touched());
        latency = _failure_model->failure_latency(p);
        ne = New NetEvent();
        assert(ne);
        ne->ts = now() + latency;
        ne->node = _nodes[p->dst()];
        ne->p = p;
        EventQueue::Instance()->here(ne);
        break;
    
      // register node on network
      case 2:
        if(_nodes[node->ip()])
          cerr << "warning: " << node->ip() << " already in network" << endl;
        _nodes[node->ip()] = node;
        break;

      default:
        break;
    }
  }
}
