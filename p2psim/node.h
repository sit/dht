#ifndef __NODE_H
#define __NODE_H

#include "p2psim.h"
#include "threaded.h"
#include "protocol.h"
#include <lib9.h>
#include <thread.h>
#include <map>
#include <string>
using namespace std;

class Node : public Threaded {
public:
  Node(IPAddress);
  virtual ~Node();

  IPAddress id() { return _id; }
  Channel *pktchan() { return _pktchan; }
  Channel *protchan() { return _protchan; }
  Protocol *getproto(string p) { return _protmap[p]; }

private:
  virtual void run();

  IPAddress _id;
  Channel *_pktchan;    // for packets
  Channel *_protchan;   // to register protocols
  map<string, Protocol*> _protmap;
};

#endif // __NODE_H
