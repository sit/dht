#include "network.h"
#include "event.h"
#include "packet.h"
#include "netevent.h"
#include "eventqueue.h"
#include <assert.h>
#include <iostream>
using namespace std;

Network *Network::_instance = 0;

Network*
Network::Instance(Topology *top)
{
  if(_instance)
    return _instance;
  return (_instance = new Network(top));
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
  chanfree(_pktchan);
  chanfree(_nodechan);
}


void
Network::run()
{
  Alt a[3];
  Packet *p;
  Node *node;
  NetEvent *ne;
  Time latency;

  a[0].c = _pktchan;
  a[0].v = &p;
  a[0].op = CHANRCV;

  a[1].c = _nodechan;
  a[1].v = &node;
  a[1].op = CHANRCV;

  a[2].op = CHANEND;
  
  while(1) {
    int i;
    if((i = alt(a)) < 0) {
      cerr << "interrupted" << endl;
      continue;
    }

    Node *dstnode;
    switch(i) {
      // get packet from network and schedule delivery
      case 0:
        dstnode = _nodes[p->_dst];
        latency = _top->latency(_nodes[p->_src], dstnode);
        ne = new NetEvent();
        ne->ts = latency;
        ne->node = dstnode;
        ne->p = p;
        send(EventQueue::Instance()->eventchan(), (Event**) &ne);
        break;
    
      // register node on network
      case 1:
        if(_nodes[node->id()])
          cerr << "warning: " << node->id() << " already in network" << endl;
        _nodes[node->id()] = node;
        break;

      default:
        break;
    }
  }
}
