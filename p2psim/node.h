#ifndef __NODE_H
#define __NODE_H

#include "threaded.h"
#include <lib9.h>
#include <thread.h>
#include <map>
#include <string>
#include "p2psim.h"
#include "packet.h"
using namespace std;

class Protocol;

class Node : public Threaded {
public:
  Node(IPAddress);
  virtual ~Node();

  IPAddress ip() { return _ip; }
  Protocol *getproto(string p) { return _protmap[p]; }
  virtual bool sendPacket(IPAddress, Packet*);

  Channel *pktchan() { return _pktchan; }
  Channel *protchan() { return _protchan; }
  void crash () { _alive = false; }
  bool alive () { return _alive; }

private:
  virtual void run();
  static void Receive(void*);

  IPAddress _ip;        // my ip address
  bool _alive;
  Channel *_pktchan;    // for packets
  Channel *_protchan;   // to register protocols

  typedef map<string,Protocol*> PM;
  typedef PM::const_iterator PMCI;
  PM _protmap;
};

#endif // __NODE_H
