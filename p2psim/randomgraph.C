#include "topology.h"
#include "protocol.h"
#include "protocolfactory.h"
#include "nodefactory.h"
#include "randomgraph.h"
#include "network.h"
#include "parse.h"
#include <iostream>
#include <fstream>
#include "p2psim.h"
#include <stdio.h>
using namespace std;

RandomGraph::RandomGraph()
  : _initialized(0), _n(0), _links(0), _routes(0)
{
}

RandomGraph::~RandomGraph()
{
  if(_links)
    free(_links);
  if(_routes)
    free(_routes);
}

// return the latency along the path from n1 to n2.
latency_t
RandomGraph::latency(Node *n1, Node *n2)
{
  initialize();

  assert(_ip2i.find(n1->ip()) != _ip2i.end());
  assert(_ip2i.find(n2->ip()) != _ip2i.end());

  int i = _ip2i[n1->ip()];
  int j = _ip2i[n2->ip()];
  assert(i < _n && j < _n && i >= 0 && j >= 0);

  int m = routes(i, j)._metric;
  assert(m >= 0 && m < 30000);

  return m;
}


void
RandomGraph::parse(ifstream &ifs)
{
  string line;
  while(getline(ifs,line)) {
    vector<string> words = split(line);

    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // nodeid, nodetype, and at least one protocol
    if(words.size() < 3) {
      cerr << "provide nodeid, nodeid, and at least one protocol per line" << endl;
      continue;
    }

    // node-id
    IPAddress ipaddr = atoi(words[0].c_str());
    if(!ipaddr)
      cerr << "found node-id 0.  you're asking for trouble." << endl;

    // what kind of node?
    Node *n = NodeFactory::Instance()->create(words[1], ipaddr);

    // all the rest are protocols on this node
    for(unsigned int i=2; i<words.size(); i++)
      send(n->protchan(), &(words[i]));

    // add the node to the network
    send(Network::Instance()->nodechan(), &n);

    // remember the new node
    if(_ip2i.find(n->ip()) != _ip2i.end()){
      cerr << "node with ip=" << ipaddr << " already added!" << endl;
      exit(1);
    }
    _ip2i[n->ip()] = _n;
    _i2node.push_back(n);
    assert(_i2node[_n] == n);
    _n++;
  }
}

// pick random links and link metrics.
// then compute routes between all nodes.
void
RandomGraph::initialize()
{
  int i, j;

  if(_initialized)
    return;
  _initialized = 1;

  _links = (short *) malloc(sizeof(short) * _n * _n);
  assert(_links);
  for(i = 0; i < _n; i++)
    for(j = 0; j < _n; j++)
      links(i, j) = -1;

  _routes = (Entry *) malloc(sizeof(Entry) * _n * _n);
  assert(_routes);
  for(i = 0; i < _n; i++){
    for(j = 0; j < _n; j++){
      routes(i, j)._next = -1;
      routes(i, j)._metric = 30000; // infinity
      routes(i, j)._hops = 30000;
    }
  }

  // five links per node
  for(i = 0; i < _n; i++){
    for(j = 0; j < degree; j++){
      int k = random() % _n;          // pick a random node.
      int m = random() % maxlatency;  // pick a random link metric.
      links(i, k) = m;
      links(k, i) = m;
    }
  }

  // set up initial tables for distance-vector.
  for(i = 0; i < _n; i++){
    routes(i, i)._next = i;
    routes(i, i)._metric = 0;
    routes(i, i)._hops = 0;
  }

  // run distance-vector until it converges.
  int iters;
  for(iters = 0; ; iters++){
    bool done = true;
    for(i = 0; i < _n; i++){
      for(j = 0; j < _n; j++){
        int m = links(i, j);
        if(m >= 0){
          // there is a link from i to j with metric m.
          // send i's routing table to j.
          int k;
          for(k = 0; k < _n; k++){
            if(routes(i, k)._next != -1 &&
               routes(i, k)._metric + m < routes(j, k)._metric){
              routes(j, k)._next = i;
              routes(j, k)._metric = routes(i, k)._metric + m;
              routes(j, k)._hops = routes(i, k)._hops + 1;
#if 0
              printf("%d to %d via %d metric %d\n",
                     j, k, i, routes(j, k)._metric);
#endif
              done = false;
            }
          }
        }
      }
    }
    if(done)
      break;
  }

  // check that the network is connected.
  // and find the average path length in milliseconds.
  double hop_sum = 0;
  double metric_sum = 0;
  double neighbors_sum = 0;
  for(i = 0; i < _n; i++){
    for(j = 0; j < _n; j++){
      if(routes(i, j)._next == -1){
        fprintf(stderr, "RandomGraph: not connected!\n");
        exit(1);
      }
      metric_sum += routes(i, j)._metric;
      hop_sum += routes(i, j)._hops;
      if(routes(i, j)._next == j && i != j)
        neighbors_sum += 1;
    }
  }
  fprintf(stderr, "dv: %d iters, avg metric %.1f, hops %.1f, degree %.1f\n",
          iters,
          metric_sum / (_n * _n),
          hop_sum / (_n * _n),
          neighbors_sum / _n);
}
