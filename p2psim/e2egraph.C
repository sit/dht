#include "topology.h"
#include "e2egraph.h"
#include "network.h"
#include "parse.h"

E2EGraph::E2EGraph(vector<string> *v)
{
  _num = atoi((*v)[0].c_str());
  assert(_num > 0);

  _pairwise.resize(_num);
  for (unsigned int i = 0; i < _num; i++) 
    _pairwise[i].resize(_num);
}

E2EGraph::~E2EGraph()
{
}

latency_t
E2EGraph::latency(IPAddress ip1, IPAddress ip2)
{
  //we can not use random ip address for e2egraph now
  assert(ip1 > 0 && ip1 <= _num);
  assert(ip2 > 0 && ip2 <= _num);
  if (ip1 < ip2)  {
    assert(_pairwise[ip1-1][ip2-1] > 0 && _pairwise[ip1-1][ip2-1] <= 1000000);
    return (latency_t) _pairwise[ip1-1][ip2-1];
  } else if (ip1 > ip2) {
    assert(_pairwise[ip2-1][ip1-1] > 0 && _pairwise[ip2-1][ip1-1] <= 1000000);
    return (latency_t) _pairwise[ip2-1][ip1-1];
  } else{
    return 0;
  }
}


void
E2EGraph::parse(ifstream &ifs)
{
  string line;

  while(getline(ifs,line)) {
    vector<string> words = split(line);
    // skip empty lines and commented lines
    if(words.empty() || words[0][0] == '#')
      continue;

    // XXX Thomer:  The else clause also seems
    // to expect 2 words per line?
    if (words[0] == "node") {
      IPAddress ipaddr = atoi(words[1].c_str());
      assert(ipaddr > 0 && ipaddr <= _num);

      // what kind of node?
      Node *n = New Node(ipaddr);

      assert(!Network::Instance()->getnode(ipaddr));

      // add the node to the network
      send(Network::Instance()->nodechan(), &n);

    } else {

      vector<string> pair = split(words[0], ",");
      IPAddress ip1 = atoi(pair[0].c_str());
      IPAddress ip2 = atoi(pair[1].c_str());
      assert(ip1>0 && ip1 <= _num);
      assert(ip2>0 && ip2 <= _num);

      int lat = atoi(words[1].c_str());
      if (lat < 0) {
	lat = 100000; //XXX missing data is treated as infinity, this is sketchy
      }
      // latency between node1 and node2
      _pairwise[ip1 -1][ip2 -1] = (latency_t) lat;
    }
  }
}
