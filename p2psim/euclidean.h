#ifndef __EUCLIDEAN_H
#define __EUCLIDEAN_H

#include <thread.h>
#include "p2psim_hashmap.h"

class Euclidean : public Topology {
public:
  Euclidean(vector<string>*);
  ~Euclidean();
  
  typedef pair<unsigned, unsigned> Coord;
  virtual void parse(ifstream&);
  virtual latency_t latency(IPAddress, IPAddress);
  Coord getcoords(IPAddress n) { return _nodes[n]; }

private:
  hash_map<IPAddress, Coord> _nodes;
  Channel *_distchan;   // to request distances
};

#endif //  __EUCLIDEAN_H
