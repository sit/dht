#ifndef __DVGRAPH_H
#define __DVGRAPH_H

// Abstract class that helps you write Topologies that
// need to compute multi-hop routes. You sub-class
// DVGraph, indicate which nodes have direct links to each
// other (and the link latencies), and DVGraph will compute
// the paths between all nodes and the path latencies.
// So DVGraph implements latency() for you.

using namespace std;

class DVGraph : public Topology {
public:
  DVGraph();
  ~DVGraph();
  
  latency_t latency(IPAddress a, IPAddress b);

 protected:
  // subclass must fill in these values by calling add_node()
  int _n; // # of nodes
  vector<IPAddress> _i2ip;
  hash_map<IPAddress, int> _ip2i;
  short *_links;   // 2-d matrix of link delays, -1 for none.

  void add_node(IPAddress a);
  short& links(int i, int j) { return _links[i*_n + j]; }
  
 private:
  int _initialized;

  // per-node forwarding tables
  struct Entry {
    int _next;
    int _metric;
    int _hops;
  };
  Entry *_routes;
  Entry& routes(int i, int j) { return _routes[i*_n + j]; }

  void dv();
};

#endif //  __DVGRAPH_H
