#include "topology.h"
#include "protocol.h"
#include "protocolfactory.h"
#include "g2graph.h"
#include "network.h"
#include "parse.h"
#include <cmath>
#include <iostream>
#include <fstream>
#include <vector>
#include "p2psim.h"

using namespace std;

/* the Gummadi brother's graph */
G2Graph::G2Graph(vector<string> *v)
{
  _num = atoi((*v)[0].c_str());
  assert(_num > 0);

  _latmap.clear();
  _samples.clear();
}

G2Graph::~G2Graph()
{
}

void
G2Graph::parse(ifstream &ifs)
{
  string line;
  while (getline(ifs,line)) {
    vector<string> words = split(line);
    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    if (words.size() == 2) {

      if (words[0] != "node") {
	cerr << "wrong G2Graph format" << endl;
      }

      IPAddress ipaddr = atoi(words[1].c_str());
      assert(ipaddr > 0 && ipaddr <= _num);

      // what kind of node?
      Node *n = New Node(ipaddr);

      // add the node to the network
      send(Network::Instance()->nodechan(), &n);

    } else { 
      int lat = atoi(words[0].c_str());
      assert(lat > 0);
      _samples.push_back(lat);
    }
  }
}

latency_t
G2Graph::latency(IPAddress ip1, IPAddress ip2)
{

  if (ip1 == ip2) return 0;

  //samples a random latency from samples
  //remembers it so future latencies for this pair will remain the same
  //also ensure symmetry
  pair<IPAddress, IPAddress>  p;
  if (ip1 < ip2) {
    p.first = ip1;
    p.second = ip2;
  }else{
    p.first = ip2;
    p.second = ip1;
  }

  if (_latmap.find(p) != _latmap.end()) {
    return _latmap[p];
  }else{

    //sample
    double r = (double)random()/(double)RAND_MAX;
    uint i = (uint) (r * _samples.size());
    _latmap[p] = _samples[i];
    return _latmap[p];
  }
}
