#ifndef __DHTPROTOCOL_H
#define __DHTPROTOCOL_H

#include "protocol.h"
#include "args.h"
using namespace std;

class Node;

class DHTProtocol : public Protocol {
public:
  DHTProtocol(Node* n);
  virtual ~DHTProtocol();

  typedef void (DHTProtocol::*event_f)(Args*);
  virtual void join(Args*) = 0;
  virtual void leave(Args*) = 0;
  virtual void crash(Args*) = 0;
  virtual void insert(Args*) = 0;
  virtual void lookup(Args*) = 0;
};

#endif // __DHTPROTOCOL_H
