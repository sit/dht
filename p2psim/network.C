#include "network.h"
#include "event.h"
#include "packet.h"
#include "netevent.h"
#include "eventqueue.h"
#include <assert.h>
#include <iostream>
#include "p2psim.h"
using namespace std;

Network *Network::_instance = 0;

Network*
Network::Instance(Topology *top)
{
  if(!_instance)
    _instance = New Network(top);
  return _instance;
}


Network::Network(Topology *top) : _top(0), _pktchan(0), _nodechan(0)
{
  _pktchan = chancreate(sizeof(Packet*), 0);
  assert(_pktchan);
  _nodechan = chancreate(sizeof(Node*), 0);
  assert(_nodechan);
  _top = top;
  assert(_top);

  // get the nodes
  thread();
}


Network::~Network()
{
  for(NMCI p = _nodes.begin(); p != _nodes.end(); ++p)
    send(p->second->exitchan(), 0);
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
  unsigned exit;

  a[0].c = _pktchan;
  a[0].v = &p;
  a[0].op = CHANRCV;

  a[1].c = _nodechan;
  a[1].v = &node;
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

    Node *dstnode, *srcnode;
    switch(i) {
      // get packet from network and schedule delivery
      case 0:
        dstnode = _nodes[p->dst()];
	srcnode = _nodes[p->src()];
	assert (dstnode);
	assert (srcnode);
        latency = _top->latency(srcnode->ip(), dstnode->ip());
        ne = New NetEvent();
        ne->ts = now() + latency;
        ne->node = dstnode;
        ne->p = p;
        send(EventQueue::Instance()->eventchan(), (Event**) &ne);
        break;
    
      // register node on network
      case 1:
        if(_nodes[node->ip()])
          cerr << "warning: " << node->ip() << " already in network" << endl;
        _nodes[node->ip()] = node;
        break;

      // exit
      case 2:
        delete this;
        break;

      default:
        break;
    }
  }
}
