#ifndef __E2EGRAPH_H
#define __E2EGRAPH_H

#include <vector>
#include <fstream>
using namespace std;

#include "topology.h"
#include "node.h"

class E2EGraph : public Topology {
public:
  E2EGraph(unsigned int num);
  ~E2EGraph();
  
  typedef pair<unsigned, unsigned> Coord;
  virtual void parse(ifstream&);
  virtual latency_t latency(Node*, Node*);

private:
  unsigned int _num;
  vector<vector<latency_t> > _pairwise;
};

#endif //  __E2EGRAPH_H
