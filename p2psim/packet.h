#ifndef __PACKET_H
#define __PACKET_H

#include <lib9.h>
#include <thread.h>
#include <string>
#include "p2psim.h"
using namespace std;

// The only thing Packet is useful for is to send RPCs,
// as managed by Node::_doRPC() and Node::Receive().
class Packet {
public:
  Packet();
  ~Packet();

  IPAddress src() { return _src; }
  IPAddress dst() { return _dst; }
  Channel *channel() { return _c; }
  bool reply() { return _fn == 0; }

private:
  // RPC function and arguments.
  // No explicit reply; put it in the args.
  void (*_fn)(void *);
  void *_args;

  friend class Node;
  Channel *_c;            // where to send the reply
  IPAddress _src;
  IPAddress _dst;
  bool _ok; // was the target node available?
};

#endif // __PACKET_H
