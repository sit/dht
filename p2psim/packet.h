#ifndef __PACKET_H
#define __PACKET_H

#include <lib9.h>
#include <thread.h>
#include "p2psim.h"
#include <string>
using namespace std;

class Packet {
public:
  // dst = destination address
  // prot
  Packet();
  ~Packet();

  //  unsigned size;
  // void (*(fn()))(void*) { return _fn; }
  // void *args() { return _args; }
  IPAddress src() { return _src; }
  IPAddress dst() { return _dst; }
  Channel *channel() { return _c; }
  bool reply() { return _fn == 0; }

private:
  // this is just like a real network packet.  different layers write their own
  // fields.

  //
  // P2P PROTOCOL LAYER
  //
  friend class Protocol;
  string _proto;             // which protocol
  void (Protocol::*_fn)(void*,void*);    // method to invoke
  void *_args;               // caller-supplied argument
  void *_ret;                // put return value in here

  //
  // NODE LAYER
  //
  friend class Node;
  Channel *_c;            // where to send the reply
  IPAddress _src;
  IPAddress _dst;
};

#endif // __PACKET_H
