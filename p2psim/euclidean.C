#include "topology.h"
#include "protocol.h"
#include "protocolfactory.h"
#include "euclidean.h"
#include "network.h"
#include <cmath>
#include <iostream>
#include <fstream>

using namespace std;

Euclidean::Euclidean()
{
}

Euclidean::~Euclidean()
{
}

latency_t
Euclidean::latency(Node *n1, Node *n2)
{
  Coord c1 = _nodes[n1->id()];
  Coord c2 = _nodes[n2->id()];

  return (latency_t) hypot(labs(c2.first - c1.first), labs(c2.second - c1.second));
}


void
Euclidean::parse(ifstream &ifs)
{
  // XXX: this is crap
  while(!ifs.eof()) {
    string node, prot;
    int id, x, y;

    // XXX: fix this bug. why didn't it find the eof earlier?
    ifs >> node;
    if(ifs.eof())
      break;

    if(node[0] == '#')
      continue;

    ifs.setf(ios::dec);
    ifs >> id >> x >> y;
    ifs.unsetf(ios::dec);
    ifs >> prot;

    // create the node
    Node *n = new Node((IPAddress) id);

    // run the specified protocol on the node
    // TODO: should be possible to run many
    Protocol *p = ProtocolFactory::Instance()->create(prot, n);
    send(n->protchan(), &prot);
    assert(p);

    // add it to the topology
    Coord c;
    c.first = x;
    c.second = y;
    if(_nodes.find(n->id()) != _nodes.end())
      cerr << "warning: node " << id << " already added!" << endl;
    _nodes[n->id()] = c;

    // add the node to the network
    send(Network::Instance()->nodechan(), &n);
  }
}
