#ifndef __P2PEVENT_H
#define __P2PEVENT_H

#include "dhtprotocol.h"
class P2PEvent : public Event {
public:
  P2PEvent();
  P2PEvent(string, vector<string>*);

  Node *node;
  string protocol;
  DHTProtocol::event_f fn;
  Args *args;

 protected:
  ~P2PEvent();

 private:
  virtual void execute();
  DHTProtocol::event_f name2fn(string name);
};

#endif // __P2PEVENT_H
