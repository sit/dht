#ifndef __EUCLIDEAN_H
#define __EUCLIDEAN_H

#include "p2psim_hashmap.h"

class Euclidean : public Topology {
public:
  Euclidean(vector<string>*);
  ~Euclidean();
  
  typedef pair<int, int> Coord;
  virtual void parse(ifstream&);
  virtual Time latency(IPAddress, IPAddress);
  Coord getcoords(IPAddress n) { return _nodes[n]; }

private:
  hash_map<IPAddress, Coord> _nodes;
};

#endif //  __EUCLIDEAN_H
