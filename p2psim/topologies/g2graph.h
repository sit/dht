#ifndef __G2GRAPH_H
#define __G2GRAPH_H

#include "p2psim/topology.h"
#include <map>
#include <string>
using namespace std;

class G2Graph : public Topology {
public:
  G2Graph(vector<string>*);
  ~G2Graph();
  
  virtual void parse(ifstream&);
  virtual Time latency(IPAddress, IPAddress);

private:
  unsigned int _num;
  map<pair<IPAddress, IPAddress>, Time> _latmap;
  vector<Time> _samples;
};

#endif //G2Graph

