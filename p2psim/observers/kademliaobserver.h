#ifndef __KADEMLIA_OBSERVER_H
#define __KADEMLIA_OBSERVER_H

#include "protocolobserver.h"
#include "../protocols/kademlia.h"

class KademliaObserver : ProtocolObserver {
public:
  KademliaObserver(Args *args);
  ~KademliaObserver();
  virtual void kick();
  virtual bool stabilized();

private:
  unsigned int _num_nodes;
  unsigned int _init_num;
  string _type;

  void init_state();

  vector<Kademlia::NodeID> lid;
};

#endif // __KADEMLIA_OBSERVER_H
