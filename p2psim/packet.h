#ifndef __PACKET_H
#define __PACKET_H

#include "p2psim.h"
#include <lib9.h>
#include <thread.h>

class Packet;
#include "protocol.h"

class Packet {
public:
  Packet();
  ~Packet();

  unsigned size;
  Protocol::msg_t type;
  Protocol::data_t data;

  NodeID src() { return _src; }
  string protocol() { return _protocol; }
  Channel *channel() { return _c; }
  bool reply() { return _reply; }

private:
  friend class Protocol;
  friend class Network;

  // the following fields can only be set by the Protocol layer
  Channel *_c;
  string _protocol;

  // the following fields can only be set by the Network layer
  NodeID _src;
  NodeID _dst;

  bool _reply;
};

#endif // __PACKET_H
