#ifndef __NETWORK_H
#define __NETWORK_H

#include "topology.h"
#include "protocol.h"
#include <list>
class Network : public Threaded {

public:
  static Network* Instance() { return Instance(0); }
  static Network* Instance(Topology*);
  Channel* pktchan() { return _pktchan; }
  Channel* nodechan() { return _nodechan; }
  Node* getnode(IPAddress id) { return _nodes[id]; }
  Topology *gettopology() { return _top; }
  list<Protocol*> getallprotocols(string);
  ~Network();

private:
  Network(Topology*);
  virtual void run();

  static Network *_instance;
  typedef hash_map<IPAddress, Node*> NM;
  typedef NM::const_iterator NMCI;
  NM _nodes;
  Topology *_top;

  Channel *_pktchan;
  Channel *_nodechan;
};

#endif // __NETWORK_H
