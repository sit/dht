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
  string _type;
  unsigned int _num_nodes;

  vector<Kademlia::NodeID> lid;
};

#endif // __KADEMLIA_OBSERVER_H
