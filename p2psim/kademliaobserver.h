#ifndef __KADEMLIA_OBSERVER_H
#define __KADEMLIA_OBSERVER_H

#include "observer.h"
#include "kademlia.h"

class KademliaObserver : public Observer {
public:
  static KademliaObserver* Instance(Args*);
  virtual void execute();

private:
  static KademliaObserver *_instance;
  KademliaObserver(Args*);
  ~KademliaObserver();
  unsigned int _reschedule;
  unsigned int _num_nodes;
  unsigned int _init_num;

  void init_state();

  vector<Kademlia::NodeID> lid;
};

#endif // __KADEMLIA_OBSERVER_H
