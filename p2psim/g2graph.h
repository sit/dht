#ifndef __G2GRAPH_H
#define __G2GRAPH_H

#include <map>
#include <string>
using namespace std;

class G2Graph : public Topology {
public:
  G2Graph(vector<string>*);
  ~G2Graph();
  
  virtual void parse(ifstream&);
  virtual latency_t latency(IPAddress, IPAddress);

private:
  unsigned int _num;
  map<pair<IPAddress, IPAddress>, latency_t> _latmap;
  vector<latency_t> _samples;
};

#endif //G2Graph

